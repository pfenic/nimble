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

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.nio.channels.UnsupportedAddressTypeException;
import java.nio.ByteBuffer;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ConnectionPendingException;
import java.nio.channels.FileChannel;
import java.nio.channels.NoConnectionPendingException;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SelectionKey;
import java.util.Optional;
import java.util.function.BiConsumer;



public class SockChannel extends AbstractTask {
	protected final EventLoop loop;
	protected final SocketChannel channel;
	protected final SelectionKey key;

	protected Runnable readHandler;
	protected Runnable writeHandler;
	protected Runnable connectHandler;


	protected SockChannel(EventLoop loop) throws IOException {
		this.loop = loop;
		this.channel = SocketChannel.open();
		this.channel.configureBlocking(false);
		this.key = channel.register(loop.selector, 0, this);
	}

	protected SockChannel(EventLoop loop, SocketChannel channel) throws IOException {
		this.loop = loop;
		this.channel = channel;
		this.channel.configureBlocking(false);
		this.key = channel.register(loop.selector, 0, this);
	}


	private void setReadHandler(Runnable handler) {
		if (this.readHandler != null) {
			throw new IllegalStateException();
		}

		this.readHandler = handler;
		this.key.interestOpsOr(SelectionKey.OP_READ);
	}

	private void setWriteHandler(Runnable handler) {
		if (this.writeHandler != null) {
			throw new IllegalStateException();
		}

		this.writeHandler = handler;
		this.key.interestOpsOr(SelectionKey.OP_WRITE);
	}

	private void removeReadHandler() {
		this.readHandler = null;
		this.key.interestOpsAnd(~SelectionKey.OP_READ);
	}

	private void removeWriteHandler() {
		this.writeHandler = null;
		this.key.interestOpsAnd(~SelectionKey.OP_WRITE);
	}


	protected void connectImpl(SocketAddress remote, BiConsumer<SockChannel, Optional<Exception>> callback) {
		try {
			this.channel.connect(remote);
		} catch (AlreadyConnectedException | ConnectionPendingException |
				UnresolvedAddressException | UnsupportedAddressTypeException |
				SecurityException | IOException e) {
			/* connect attempt failed -> invoke callback and return */
			this.loop.setImmediate(() -> callback.accept(null, Optional.of(e)));
			return;
		}

		this.connectHandler = () -> {
			Exception exception = null;
			try {
				boolean ret = this.channel.finishConnect();
				if (!ret) {
					/* connecting not finished yet -> try again later */
					return;
				}
			} catch (NoConnectionPendingException | IOException e) {
				/* connect attempt failed -> invoke callback and return */
				exception = e;
			}

			callback.accept(this, Optional.ofNullable(exception));
			this.key.interestOpsAnd(~SelectionKey.OP_CONNECT);
			this.connectHandler = null;
		};

		this.key.interestOpsOr(SelectionKey.OP_CONNECT);
	}

	protected void readImpl(ByteBuffer buf, int n, BiConsumer<Integer, Optional<Exception>> callback) {
		final boolean returnImmediately = n == 0;
		final int remaining = buf.remaining();
		final int N = ((n < remaining) & !returnImmediately) ? n : remaining;
		final var readBuf = buf.slice();
		readBuf.limit(N);

		setReadHandler(() -> {
			Exception exception = null;

			try {
				int ret = channel.read(readBuf);
				if (ret == -1) {
					exception = new EOFException();
				} else if (readBuf.hasRemaining() & !(returnImmediately & (ret > 0))) {
					// buf not full or nothing read -> read again
					return;
				}
			} catch (NotYetConnectedException | IOException e) {
				exception = e;
			}

			int readN = readBuf.position();
			buf.position(buf.position() + readN);
			removeReadHandler();
			callback.accept(readN, Optional.ofNullable(exception));
		});
	}

	protected void writeImpl(ByteBuffer buf, int n, BiConsumer<Integer, Optional<Exception>> callback) {
		final boolean returnImmediately = n == 0;
		final int remaining = buf.remaining();
		final int N = ((n < remaining) & !returnImmediately) ? n : remaining;
		final var writeBuf = buf.slice();
		writeBuf.limit(N);

		setWriteHandler(() -> {
			Exception exception = null;

			try {
				int ret = channel.write(writeBuf);
				if (writeBuf.hasRemaining() & !(returnImmediately & (ret > 0))) {
					// buf not completely written or nothing written -> write again
					return;
				}
			} catch (NotYetConnectedException | IOException e) {
				exception = e;
			}

			int wroteN = writeBuf.position();
			buf.position(buf.position() + wroteN);
			removeWriteHandler();
			callback.accept(wroteN, Optional.ofNullable(exception));
		});
	}

	protected void transferToImpl(FileChannel file, long pos, long n, BiConsumer<Long, Optional<Exception>> callback) {
		long size;
		try {
			size = file.size();
		} catch (IOException e) {
			this.loop.setImmediate(() -> callback.accept(0L, Optional.of(e)));
			return;
		}

		final boolean returnImmediately = n == 0;
		final long remaining = size - pos < 0 ? 0 : size - pos;
		n = n == -1 ? remaining : n;
		final long N = ((n < remaining) & !returnImmediately) ? n : remaining;

		setReadHandler(new Runnable() {
			private long read = 0;

			@Override
			public void run() {
				Exception exception = null;

				try {
					// FIXME can this cause an infinite loop if channel is closed / EOF reached ?
					long ret = file.transferFrom(channel, pos + this.read, N - this.read);
					this.read += ret;
					if (this.read < N & !(returnImmediately & (ret > 0))) {
						// not completely read or nothing read -> write again
						return;
					}
				} catch (NonReadableChannelException | NonWritableChannelException | IOException e) {
					exception = e;
				} catch (IllegalArgumentException e) {
					System.out.println(e);
					exception = e;
				}

				// TODO untested
				removeReadHandler();
				callback.accept(this.read, Optional.ofNullable(exception));
			}
		});
	}

	protected void transferFromImpl(FileChannel file, long pos, long n, BiConsumer<Long, Optional<Exception>> callback) {
		long size;
		try {
			size = file.size();
		} catch (IOException e) {
			this.loop.setImmediate(() -> callback.accept(0L, Optional.of(e)));
			return;
		}

		final boolean returnImmediately = n == 0;
		final long remaining = size - pos < 0 ? 0 : size - pos;
		n = n == -1 ? remaining : n;
		final long N = ((n < remaining) & !returnImmediately) ? n : remaining;

		setWriteHandler(new Runnable() {
			private long written = 0;

			@Override
			public void run() {
				Exception exception = null;

				try {
					long ret = file.transferTo(pos + this.written, N - this.written, channel);
					this.written += ret;
					if (this.written < N & !(returnImmediately & (ret > 0))) {
						// not completely written or nothing written -> write again
						return;
					}
				} catch (NonReadableChannelException | NonWritableChannelException | IOException e) {
					exception = e;
				} catch (IllegalArgumentException e) {
					System.out.println(e);
					exception = e;
				}

				removeWriteHandler();
				callback.accept(this.written, Optional.ofNullable(exception));
			}
		});
	}

	@Override
	protected void run() {
		try {
			if (this.key.isReadable()) {
				System.out.println("read");
				this.readHandler.run();
			}
			if (this.key.isWritable()) {
				System.out.println("write");
				this.writeHandler.run();
			}
			if (this.key.isConnectable()) {
				System.out.println("connect");
				this.connectHandler.run();
			}
		} catch (CancelledKeyException e) {
			/* ignored */
			// TODO check if 'isClosed' == false -> unexpected, throw exception
		}
	}

	protected void closeImpl() {
		this.key.cancel();
		try {
			this.channel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// TODO implement close on finished
	}


	public void close() {
		closeImpl();
	}

	public <A> void connect(SocketAddress remote, A attachment, CallbackAttached<SockChannel, A> callback) {
		connectImpl(remote, (res, fail) -> callback.run(res, fail, attachment));
	}

	public void connect(SocketAddress remote, Callback<SockChannel> callback) {
		connectImpl(remote, (res, fail) -> callback.run(res, fail));
	}

	// TODO bind()

	/**
	 * Reads until the buffer is filled, before invoking the callback.
	 *
	 * @param buf
	 * @param callback
	 */
	public <A> void read(ByteBuffer buf, A attachment, CallbackAttached<Integer, A> callback) {
		readImpl(buf, buf.remaining(), (res, fail) -> callback.run(res, fail, attachment));
	}

	public void read(ByteBuffer buf, Callback<Integer> callback) {
		readImpl(buf, buf.remaining(), (res, fail) -> callback.run(res, fail));
	}

	/**
	 * Reads until n bytes have been read, or if n equals 0, until at least one byte has been read,
	 * before invoking the callback.
	 *
	 * @param buf
	 * @param n
	 * @param callback
	 */
	public <A> void read(ByteBuffer buf, int n, A attachment, CallbackAttached<Integer, A> callback) {
		readImpl(buf, n, (res, fail) -> callback.run(res, fail, attachment));
	}

	public void read(ByteBuffer buf, int n, Callback<Integer> callback) {
		readImpl(buf, n, (res, fail) -> callback.run(res, fail));
	}

	/**
	 * Write until the buffer is empty, before invoking the callback.
	 *
	 * @param buf
	 * @param callback
	 */
	public <A> void write(ByteBuffer buf, A attachment, CallbackAttached<Integer, A> callback) {
		writeImpl(buf, buf.remaining(), (res, fail) -> callback.run(res, fail, attachment));
	}

	public void write(ByteBuffer buf, Callback<Integer> callback) {
		writeImpl(buf, buf.remaining(), (res, fail) -> callback.run(res, fail));
	}

	/**
	 * Write until n bytes have been written, or if n equals 0, until at least one byte has been written,
	 * before invoking the callback.
	 *
	 * @param buf
	 * @param n
	 * @param callback
	 */
	public <A> void write(ByteBuffer buf, int n, A attachment, CallbackAttached<Integer, A> callback) {
		writeImpl(buf, n, (res, fail) -> callback.run(res, fail, attachment));
	}

	public void write(ByteBuffer buf, int n, Callback<Integer> callback) {
		writeImpl(buf, n, (res, fail) -> callback.run(res, fail));
	}

	/**
	 * (read) Read from this {@link IOSockChannel} and transfer to file.
	 * @param file
	 */
	public <A> void transferTo(FileChannel file, A attachment, CallbackAttached<Long, A> callback) {
		transferToImpl(file, 0, -1, (res, fail) -> callback.run(res, fail, attachment));
	}

	public void transferTo(FileChannel file, Callback<Long> callback) {
		transferToImpl(file, 0, -1, (res, fail) -> callback.run(res, fail));
	}

	public <A> void transferTo(FileChannel file, long pos, long n, A attachment, CallbackAttached<Long, A> callback) {
		if (pos < 0 | n < 0) {
			throw new IllegalArgumentException();
		}

		transferToImpl(file, pos, n, (res, fail) -> callback.run(res, fail, attachment));
	}

	public void transferTo(FileChannel file, long pos, long n, Callback<Long> callback) {
		if (pos < 0 | n < 0) {
			throw new IllegalArgumentException();
		}

		transferToImpl(file, pos, n, (res, fail) -> callback.run(res, fail));
	}

	/**
	 * (write) Transfer from file and write to this {@link IOSockChannel}.
	 * @param file
	 */
	public <A> void transferFrom(FileChannel file, A attachment, CallbackAttached<Long, A> callback) {
		transferFromImpl(file, 0, -1, (res, fail) -> callback.run(res, fail, attachment));
	}

	public void transferFrom(FileChannel file, Callback<Long> callback) {
		transferFromImpl(file, 0, -1, (res, fail) -> callback.run(res, fail));
	}

	public <A> void transferFrom(FileChannel file, long pos, long n, A attachment, CallbackAttached<Long, A> callback) {
		if (pos < 0 | n < 0) {
			throw new IllegalArgumentException();
		}

		transferFromImpl(file, pos, n, (res, fail) -> callback.run(res, fail, attachment));
	}

	public void transferFrom(FileChannel file, long pos, long n, Callback<Long> callback) {
		if (pos < 0 | n < 0) {
			throw new IllegalArgumentException();
		}

		transferFromImpl(file, pos, n, (res, fail) -> callback.run(res, fail));
	}
}

