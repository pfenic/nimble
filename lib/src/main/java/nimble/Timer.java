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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;



public class Timer <T> {
	private class TimerTask {
		public final T task;
		public final long countdown;

		public long remaining;
		public boolean periodic;


		public TimerTask(T task, long countdown, boolean periodic) {
			this.task = task;
			this.countdown = countdown;
			this.periodic = periodic;
		}
	}


	private long lastTick;
	private long interval;
	private long elapsed;
	private final ArrayList<TimerTask> taskList;
	private final HashSet<T> cancelSet;
	private HashSet<T> readySet;


	// interval in ms
	public Timer(long interval) {
		this.lastTick = System.nanoTime();
		this.interval = interval * 1000000;
		this.taskList = new ArrayList<>();
		this.cancelSet = new HashSet<>();
		this.readySet = new HashSet<>();
	}


	private void insert(TimerTask timerTask) {
		timerTask.remaining = timerTask.countdown;

		for (int i = 0; i < this.taskList.size(); i++) {
			var current = this.taskList.get(i);
			long remaining = timerTask.remaining - current.remaining;

			if (remaining <= 0) {
				current.remaining -= timerTask.remaining;
				this.taskList.add(i, timerTask);
				return;
			}

			timerTask.remaining = remaining;
		}

		this.taskList.add(timerTask);
	}


	// TODO better name?
	public Set<T> readySet() {
		final var ret = this.readySet;

		this.readySet = new HashSet<>();

		return ret;
	}

	public int tick() {
		long now = System.nanoTime();
		long dt = now - this.lastTick;
		this.lastTick = now;
		this.elapsed += dt;

		if (this.elapsed < this.interval) {
			return this.readySet.size();
		}

		long ticks = this.elapsed;
		this.elapsed = 0;

		int i = 0;
		var rescheduleList = new ArrayList<TimerTask>();

		for ( ; i < this.taskList.size(); i++) {
			var timerTask = this.taskList.get(i);
			timerTask.remaining -= ticks;

			if (timerTask.remaining > 0) {
				break;
			}

			this.taskList.remove(i);
			ticks = -timerTask.remaining;

			T task = timerTask.task;
			if (!this.cancelSet.remove(task)) {
				this.readySet.add(task);
				if (timerTask.periodic) {
					rescheduleList.add(timerTask);
				}
			}
		}

		rescheduleList.forEach(t -> insert(t));

		return this.readySet.size();
	}

	public void schedule(T task, long countdown, boolean periodic) {
		var timerTask = new TimerTask(task, countdown * 1000000L, periodic);
		insert(timerTask);
	}

	public void cancel(T task) {
		this.cancelSet.add(task);
	}
}
