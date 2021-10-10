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

import java.io.File;
import java.io.IOException;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.util.function.BiConsumer;
import java.util.Optional;
import java.nio.channels.SelectionKey;



public class SSLServerSockChannel extends ServerSockChannel {
	private final SSLContext sslContext;


	protected SSLServerSockChannel(EventLoop loop) throws IOException {
		super(loop);

		char[] passphrase = "testserverpw".toCharArray();

		// TODO move outside of constructor - give interface for user provided "SSLContext" etc.
		try {
			KeyStore ksKeys = KeyStore.getInstance(new File("keystore.ks"), passphrase);
			KeyStore ksTrust = KeyStore.getInstance("JKS");
			ksTrust.load(null, null);
			KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
			kmf.init(ksKeys, passphrase);
			TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
			tmf.init(ksTrust);
			sslContext = SSLContext.getInstance("TLS");
			sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}


	@Override
	protected void acceptImpl(BiConsumer<SockChannel, Optional<Exception>> callback) {
		super.handler = () -> {
			try {
				final var sock = this.channel.accept();
				// TODO hostname and port?
				var sslEngine = sslContext.createSSLEngine();
				sslEngine.setUseClientMode(false);
				final var sslSockChannel = new SSLSockChannel(this.loop, sslEngine, sock);
				/* initiate handshake process */
				sslSockChannel.initHandshake();
				/* invoke callback; error during handshake will be reported to first r/w */
				callback.accept(sslSockChannel, Optional.empty());
			} catch (Exception e) {
				callback.accept(null, Optional.of(e));
			}
		};

		this.key.interestOps(SelectionKey.OP_ACCEPT);
	}
}
