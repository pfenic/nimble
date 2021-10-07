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



public abstract class TimerTask extends AbstractTask {
	protected TimerTask() {
		init();
	}

	protected abstract void init();

	public void cancel() { }
}

