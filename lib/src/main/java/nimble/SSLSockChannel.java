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

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ExecutorService;
import java.io.EOFException;
import java.io.IOException;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;



public class SSLSockChannel extends SockChannel {
	private static ExecutorService pool = Executors.newCachedThreadPool();

	private final SSLEngine sslEngine;
	private final SSLSession sslSession;

	private HandshakeStatus handshakeStatus;
	private boolean handshakeInProgress;
	private boolean connectionClosed;

	private ByteBuffer outboundAppData;
	private ByteBuffer inboundAppData;
	private ByteBuffer outboundNetData;
	private ByteBuffer inboundNetData;

	private Exception bufferedException;

	private Runnable readHandler;
	private Runnable writeHandler;


	// TODO client socket
	protected SSLSockChannel(EventLoop loop, SSLEngine sslEngine, SocketChannel channel) throws IOException {
		super(loop, channel);
		this.sslEngine = sslEngine;
		this.sslSession = sslEngine.getSession(); // TODO session after handshake is finished?
		this.handshakeInProgress = false;
		this.connectionClosed = false;

		int appBufferSize = this.sslSession.getApplicationBufferSize();
		int packetBufferSize = this.sslSession.getPacketBufferSize();
		this.outboundAppData = ByteBuffer.allocate(appBufferSize);
		this.inboundAppData = ByteBuffer.allocate(appBufferSize);
		this.outboundNetData = ByteBuffer.allocate(packetBufferSize);
		this.inboundNetData = ByteBuffer.allocate(packetBufferSize);

		this.inboundAppData.flip();
		this.outboundNetData.flip();

		this.bufferedException = null;
	}


	private void setReadHandler(Runnable handler) {
		if (this.readHandler != null) {
			throw new IllegalStateException();
		}

		this.readHandler = handler;

		if (!this.handshakeInProgress) {
			//this.loop.scheduleTask(handler);
			handler.run();
		}
	}

	private void setWriteHandler(Runnable handler) {
		if (this.writeHandler != null) {
			throw new IllegalStateException();
		}

		this.writeHandler = handler;

		if (!this.handshakeInProgress) {
			//this.loop.scheduleTask(handler);
			handler.run();
		}
	}

	private void removeReadHandler() {
		this.readHandler = null;
	}

	private void removeWriteHandler() {
		this.writeHandler = null;
	}

	private void handshakeStep() {
		switch (this.handshakeStatus) {
			case NEED_WRAP:
				System.out.println("NEED_WRAP");
				try {
					wrap();
				} catch (SSLException | ClosedChannelException e) {
					abortHandshake(e);
					return;
				}

				super.writeImpl(this.outboundNetData, this.outboundNetData.remaining(), (res, failed) -> {
					if (failed.isPresent()) {
						abortHandshake(failed.get());
						return;
					}

					if (this.handshakeStatus != HandshakeStatus.FINISHED) {
						handshakeStep();
					} else {
						finishHandshake();
					}
				});
				break;
			case NEED_UNWRAP:
				System.out.println("NEED_UNWRAP");
				super.readImpl(this.inboundNetData, 0, (res, failed) -> {
					if (failed.isPresent()) {
						abortHandshake(failed.get());
						return;
					}

					try {
						unwrap();
					} catch (SSLException | ClosedChannelException e) {
						abortHandshake(e);
						return;
					}

					if (this.handshakeStatus != HandshakeStatus.FINISHED) {
						handshakeStep();
					} else {
						finishHandshake();
					}
				});
				break;
			case NEED_TASK:
				System.out.println("NEED_TASK");
				var countdown = new AtomicLong(Long.MAX_VALUE);
				long nTasks = 0;
				while (true) {
					Runnable task = this.sslEngine.getDelegatedTask();
					if (task == null) {
						break;
					}
					nTasks++;
					pool.execute(() -> {
						task.run();
						if (countdown.decrementAndGet() == 0) {
							this.loop.syncedSetImmediate(() -> {
								this.handshakeStatus = this.sslEngine.getHandshakeStatus();
								handshakeStep();
							});
						}
					});
				}
				if (countdown.addAndGet(- (Long.MAX_VALUE - nTasks)) == 0) {
					this.handshakeStatus = this.sslEngine.getHandshakeStatus();
					handshakeStep();
				}
				break;
			default:
				throw new IllegalStateException();
		}
	}

	private void abortHandshake(Exception e) {
		this.handshakeInProgress = false;
		this.bufferedException = e;

		if (this.readHandler != null) {
			this.readHandler.run();
		} else if (this.writeHandler != null) {
			this.writeHandler.run();
		}
	}

	private void finishHandshake() {
		this.handshakeInProgress = false;
		System.out.println("Handshake finished!");
		if (this.readHandler != null) {
			try {
				unwrap();
			} catch (SSLException | ClosedChannelException e) {
				this.bufferedException = e;
				this.readHandler.run();
				return;
			}

			if (this.handshakeStatus != HandshakeStatus.NOT_HANDSHAKING) {
				this.handshakeInProgress = true;
				handshakeStep();
				return;
			}

			this.readHandler.run();
		}

		if (this.writeHandler != null) {
			this.writeHandler.run();
		}
	}

	private void wrap() throws SSLException, ClosedChannelException {
		this.outboundAppData.flip();
		this.outboundNetData.compact();

		SSLEngineResult res;
		try {
			res = this.sslEngine.wrap(outboundAppData, this.outboundNetData);
			this.handshakeStatus = res.getHandshakeStatus();

			// TODO fix cases and error handling
			switch (res.getStatus()) {
				case BUFFER_OVERFLOW:
					if (this.sslSession.getPacketBufferSize() > this.outboundNetData.capacity()) {
						var buffer = ByteBuffer.allocate(this.sslSession.getPacketBufferSize());
						buffer.put(this.outboundNetData);
						this.outboundNetData = buffer;
						this.outboundNetData.flip();
						System.out.println("wrap: overflow resize");
						/* retry */
						//wrap(); // TODO retry
						//return;
						System.exit(1);
					}
					/* send outboundNetData */
					break;
				case BUFFER_UNDERFLOW:
					// input comes from user, unless buffer.capacity == 0 (which should be tested before (TODO)),
					// an underflow makes no sense
					throw new IllegalStateException();
				case CLOSED:
					// TODO handle closed?
					throw new ClosedChannelException();
				case OK:
					/* nothing to do */
					break;
			}
		} finally {
			this.outboundAppData.compact();
			this.outboundNetData.flip();
		}
	}

	private void unwrap() throws SSLException, ClosedChannelException {
		this.inboundAppData.compact();
		this.inboundNetData.flip();

		SSLEngineResult res;
		try {
			res = this.sslEngine.unwrap(this.inboundNetData, this.inboundAppData);
			this.handshakeStatus = res.getHandshakeStatus();

			// TODO fix cases and error handling
			switch (res.getStatus()) {
				case BUFFER_OVERFLOW:
					if (this.sslSession.getApplicationBufferSize() > this.inboundAppData.capacity()) {
						var buffer = ByteBuffer.allocate(this.sslSession.getApplicationBufferSize());
						buffer.put(this.inboundAppData);
						this.inboundAppData = buffer;
						this.inboundAppData.flip();
						System.out.println("unwrap: overflow resize");
						/* retry */
						//unwrap(); // TODO retry
						// return;
						System.exit(1);
					}
					// buffer full -> empty into readBuf
					break;
				case BUFFER_UNDERFLOW:
					if (this.sslSession.getPacketBufferSize() > this.inboundNetData.capacity()) {
						var buffer = ByteBuffer.allocate(this.sslSession.getPacketBufferSize());
						this.inboundNetData.flip();
						buffer.put(this.inboundNetData);
						this.inboundNetData = buffer;
						System.out.println("unwrap: underflow resize");
						/* retry */
						// XXX probably makes no sense
						//unwrap(); // TODO retry
						//return;
						System.exit(1);
					}
					// not enough data available -> needs to receive more from peer
					break;
				case CLOSED:
					// TODO set channel closed boolean
					throw new ClosedChannelException();
				case OK:
					/* nothing to do */
					break;
			}
		} finally {
			this.inboundAppData.flip();
			this.inboundNetData.compact();
		}
	}

	private void receive(Runnable callback) {
		super.readImpl(this.inboundNetData, 0, (res, failed) -> {
			if (failed.isPresent()) {
				this.bufferedException = failed.get();
				callback.run();
				return;
			}

			try {
				unwrap();
			} catch (SSLException | ClosedChannelException e) {
				this.bufferedException = e;
				callback.run();
				return;
			}

			if (this.handshakeStatus != HandshakeStatus.NOT_HANDSHAKING) {
				if (!this.handshakeInProgress) {
					this.handshakeInProgress = true;
					handshakeStep();
				}
			} else {
				callback.run();
			}
		});
	}

	private void send(Runnable callback) {
		try {
			wrap();
		} catch (SSLException | ClosedChannelException e) {
			this.bufferedException = e;
			callback.run();
			return;
		}

		if (this.handshakeStatus != HandshakeStatus.NOT_HANDSHAKING) {
			if (!this.handshakeInProgress) {
				this.handshakeInProgress = true;
				handshakeStep();
			}
		} else {
			super.writeImpl(this.outboundNetData, this.outboundNetData.remaining(), (res, failed) -> {
				if (failed.isPresent()) {
					this.bufferedException = failed.get();
				}

				callback.run();
			});
		}

	}


	protected void initHandshake() throws SSLException {
		if (this.handshakeInProgress) {
			throw new IllegalStateException();
		}

		this.handshakeInProgress = true;

		this.sslEngine.beginHandshake();
		this.handshakeStatus = this.sslEngine.getHandshakeStatus();

		handshakeStep();
	}

	@Override
	protected void closeImpl() {
		// TODO shut down input and output properly
		this.connectionClosed = true;
		super.closeImpl();
	}

	@Override
	protected void readImpl(ByteBuffer buf, int n, BiConsumer<Integer, Optional<Exception>> callback) {
		if (!buf.hasRemaining()) {
			throw new IllegalArgumentException();
		}

		final boolean returnImmediately = n == 0;
		final int remaining = buf.remaining();
		final int N = ((n < remaining) & !returnImmediately) ? n : remaining;
		final var readBuf = buf.slice();
		readBuf.limit(N);

		setReadHandler(new Runnable() {
			@Override
			public void run() {
				Exception exception = null;

				if (SSLSockChannel.this.bufferedException != null) {
					exception = SSLSockChannel.this.bufferedException;
					SSLSockChannel.this.bufferedException = null;
				} else {
					final int available = SSLSockChannel.this.inboundAppData.remaining();
					final int copyN = available < readBuf.remaining() ? available : readBuf.remaining();
					final int position = SSLSockChannel.this.inboundAppData.position();
					readBuf.put(SSLSockChannel.this.inboundAppData.array(), position, copyN);
					SSLSockChannel.this.inboundAppData.position(position + copyN);
					System.out.println("[readImpl] copied: " + copyN);
					if (copyN == 0 | (readBuf.hasRemaining() & !returnImmediately)) {
						// buf not full -> read again
						receive(this);
						return;
					}
				}

				int readN = readBuf.position();
				buf.position(buf.position() + readN);
				var failed = Optional.ofNullable(exception);
				SSLSockChannel.this.loop.setImmediate(() -> {
					removeReadHandler();
					callback.accept(readN, failed);
				});
			}
		});
	}

	@Override
	protected void writeImpl(ByteBuffer buf, int n, BiConsumer<Integer, Optional<Exception>> callback) {
		if (!buf.hasRemaining()) {
			throw new IllegalArgumentException();
		}

		final boolean returnImmediately = n == 0;
		final int remaining = buf.remaining();
		final int N = ((n < remaining) & !returnImmediately) ? n : remaining;
		final var writeBuf = buf.slice();
		writeBuf.limit(N);

		setWriteHandler(new Runnable() {
			@Override
			public void run() {
				Runnable finish = () -> {
					int wroteN = writeBuf.position();
					buf.position(buf.position() + wroteN);
					var failed = Optional.ofNullable(SSLSockChannel.this.bufferedException);
					SSLSockChannel.this.bufferedException = null;

					SSLSockChannel.this.loop.setImmediate(() -> {
						removeWriteHandler();
						callback.accept(wroteN, failed);
					});
				};

				if (SSLSockChannel.this.bufferedException != null) {
					finish.run();
				} else {
					final int available = SSLSockChannel.this.outboundAppData.remaining();
					final int copyN = available < writeBuf.remaining() ? available : writeBuf.remaining();
					final int position = SSLSockChannel.this.outboundAppData.position();
					writeBuf.get(SSLSockChannel.this.outboundAppData.array(), position, copyN);
					SSLSockChannel.this.outboundAppData.position(position + copyN);
					System.out.println("[writeImpl] copied: " + copyN);
					if (copyN == 0 | (writeBuf.hasRemaining() & !returnImmediately)) {
						// buf not empty -> write again
						send(this);
					} else {
						send(finish);
					}
				}
			}
		});
	}

	@Override
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

		System.out.println("transferToImpl: not implement");
		System.exit(1);
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
				//} catch (NonReadableChannelException | NonWritableChannelException | IOException e) {
				} catch (Exception e) {
					exception = e;
				//} catch (IllegalArgumentException e) {
					System.out.println(e);
					exception = e;
				}
				// TODO untested

				//removeReadHandler();
				callback.accept(this.read, Optional.ofNullable(exception));
			}
		});

		this.key.interestOpsOr(SelectionKey.OP_READ);
	}

	@Override
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
				Runnable finish = () -> {
					var failed = Optional.ofNullable(SSLSockChannel.this.bufferedException);
					SSLSockChannel.this.bufferedException = null;
					SSLSockChannel.this.loop.setImmediate(() -> {
						removeWriteHandler();
						callback.accept(this.written, failed);
					});
				};

				if (SSLSockChannel.this.bufferedException != null) {
					finish.run();
				} else {
					int ret;
					try {
						// TODO only read up to N bytes (use slice? or map?)
						ret = file.read(SSLSockChannel.this.outboundAppData);
						System.out.println("[transfF] copied: " + ret);
						if (ret == -1) {
							new EOFException("This should not happen!").printStackTrace();
							System.exit(1);
						}
						written += ret;
						if (this.written < N & !(returnImmediately & (ret > 0))) {
							// not completely written or nothing written -> write again
							send(this);
						} else {
							send(finish);
						}
					} catch (IOException e) {
						SSLSockChannel.this.loop.setImmediate(() -> {
							removeWriteHandler();
							callback.accept(this.written, Optional.of(e));
						});
					}
				}
			}
		});
	}
}
