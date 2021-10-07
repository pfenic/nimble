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
import java.nio.channels.Selector;
import java.util.HashSet;
import java.util.function.Consumer;



public class EventLoop {
	private static final ThreadLocal<EventLoop> instance = new ThreadLocal<EventLoop>();


	/**
	* Allocates the thread local {@link IOEventLoop} instance.
	* @throws IOException
	 */
	public static void init() throws IOException {
		if (instance.get() == null) {
			instance.set(new EventLoop());
		}
	}


	/**
	* Get the thread local {@link IOEventLoop} instance.
	*
	* @return
	 */
	public static EventLoop getInstance() {
		if (instance.get() == null) {
			try {
				instance.set(new EventLoop());
			} catch (IOException e) {
				return null;
			}
		}

		return instance.get();
	}


	private final Timer<TimerTask> timer;
	private final HashSet<AbstractTask> readySet; // TODO maybe change name?

	private boolean running;


	protected final Selector selector;


	private EventLoop() throws IOException {
		this.selector = Selector.open();
		this.readySet = new HashSet<>();
		this.timer = new Timer<>(1);
		this.running = true;
	}


	private TimerTask setTimeTask(long ms, boolean periodic, Runnable task) {
		return new TimerTask() {
			@Override
			protected void init() {
				EventLoop.this.timer.schedule(this, ms, periodic);
			}

			@Override
			protected void run() {
				task.run();
			}

			@Override
			public void cancel() {
				EventLoop.this.timer.cancel(this);
			}
		};
	}


	protected void scheduleTask(AbstractTask task) {
		this.readySet.add(task);
	}


	public SockChannel openSockChannel() throws IOException {
		return new SockChannel(this);
	}

	public ServerSockChannel openServerSockChannel() throws IOException {
		return new ServerSockChannel(this);
	}
	

	public <A> void setImmediate(A attachment, Consumer<A> task) {
		setImmediate(() -> task.accept(attachment));
	}

	public void setImmediate(Runnable task) {
		new TimerTask() {
			@Override
			protected void init() {
				scheduleTask(this);
			}

			@Override
			protected void run() {
				task.run();
			}
		};
	}

	public <A> TimerTask setAfter(long ms, A attachment, Consumer<A> task) {
		return setTimeTask(ms, false, () -> task.accept(attachment));
	}

	public TimerTask setAfter(long ms, Runnable task) {
		return setTimeTask(ms, false, task);
	}

	public <A> TimerTask setPeriodic(long ms, A attachment, Consumer<A> task) {
		return setTimeTask(ms, true, () -> task.accept(attachment));
	}

	public TimerTask setPeriodic(long ms, Runnable task) {
		return setTimeTask(ms, true, task);
	}


	public void start() throws IOException {
		var tasks = new HashSet<AbstractTask>();

		// TODO catch exceptions thrown by select()? handle how?
		while (running) {
			this.timer.tick();
			final var timerTasks = this.timer.readySet();
			tasks.addAll(timerTasks);

			tasks.addAll(this.readySet);
			this.readySet.clear();

			if (tasks.isEmpty()) {
				selector.select(1);
			} else {
				selector.selectNow();
			}
			final var keys = selector.selectedKeys();
			keys.stream()
				.map(key -> (AbstractTask) key.attachment())
				.forEach(tasks::add);
			keys.clear();

			for (final var task : tasks) {
				task.run();
			}

			tasks.clear();
		}
	}

	public void stop() {
		this.running = false;
	}
}

