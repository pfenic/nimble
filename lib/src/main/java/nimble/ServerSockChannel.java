/*
 * Copyright (C) 2021 Nicolas Pfeiffer
 *
 * This file is part of Nimble.
 *
 * Nimble is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Nimble is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Nimble.  If not, see <https://www.gnu.org/licenses/>.
 */

package nimble;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.util.Optional;
import java.util.function.BiConsumer;



public class ServerSockChannel extends AbstractTask {
	protected final EventLoop loop;
	protected final ServerSocketChannel channel;
	protected final SelectionKey key;
	protected Runnable handler;


	protected ServerSockChannel(EventLoop loop) throws IOException {
		this.loop = loop;
		this.channel = ServerSocketChannel.open();
		this.channel.configureBlocking(false);
		this.key = channel.register(loop.selector, 0, this);
	}


	protected void acceptImpl(BiConsumer<SockChannel, Optional<Exception>> callback) {
		this.handler = () -> {
			try {
				final var sock = this.channel.accept();
				final var sockChannel = new SockChannel(this.loop, sock);
				callback.accept(sockChannel, Optional.empty());
			} catch (Exception e) {
				callback.accept(null, Optional.of(e));
			}
		};

		this.key.interestOps(SelectionKey.OP_ACCEPT);
	}

	@Override
	protected void run() {
		try {
			if (this.key.isAcceptable()) {
				System.out.println("accept");
				this.handler.run();
			}
		} catch (CancelledKeyException e) {
			/* ignored */
			// TODO is it better to check key.isValid() before isReadable()...?
		}
	}


	/**
	 * Stop accepting new connections and close the channel.
	 */
	public void close() {
		this.key.cancel();
		try {
			this.channel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Bind this channel to {@link SocketAddress} local.
	 *
	 * @param local
	 */
	public <A> void bind(SocketAddress local) throws IOException {
		this.channel.bind(local);
	}

	/**
	 * Sets or updates the callback to invoke upon accepting a new connection,
	 * and starts accepting new connections. To stop accepting new connections
	 * call the cancel() method of this class.
	 *
	 * @param callback
	 */
	public <A> void accept(A attachment, CallbackAttached<SockChannel, A> callback) {
		acceptImpl((res, fail) -> callback.run(res, fail, attachment));
	}

	public void accept(Callback<SockChannel> callback) {
		acceptImpl((res, fail) -> callback.run(res, fail));
	}

	/**
	 * Stop accepting new connections.
	 */
	public void cancel() {
		this.key.interestOps(0);
		this.handler = null;
	}
}
