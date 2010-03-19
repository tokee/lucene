/* $Id:$
 *
 * The Summa project.
 * Copyright (C) 2005-2010  The State and University Library
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.apache.lucene.util.packed;

/**
 * Simple wrapper for a PackedInts Mutable or Reader, subtracting the given
 * delta from the value for all calls to set and adding the delta before
 * returning the value from gets.
 */
public class DeltaWrapper {
  public static PackedInts.Mutable wrap(PackedInts.Mutable mutable, int delta) {
    return new DeltaMutable(mutable, delta);
  }
  public static PackedInts.Reader wrap(PackedInts.Reader reader, int delta) {
    return new DeltaReader(reader, delta);
  }

  private static class DeltaMutable implements PackedInts.Mutable {
    private PackedInts.Mutable mutable;
    private int delta;

    private DeltaMutable(PackedInts.Mutable mutable, int delta) {
      this.mutable = mutable;
      this.delta = delta;
    }

    public void set(int index, long value) {
      mutable.set(index, value - delta);
    }

    public void clear() {
      mutable.clear();
    }

    public long get(int index) {
      return mutable.get(index) + delta;
    }

    public int getBitsPerValue() {
      return mutable.getBitsPerValue();
    }

    public int size() {
      return mutable.size();
    }
  }

  private static class DeltaReader implements PackedInts.Reader {
    private PackedInts.Reader reader;
    private int delta;

    private DeltaReader(PackedInts.Reader reader, int delta) {
      this.reader = reader;
      this.delta = delta;
    }

    public long get(int index) {
      return reader.get(index) + delta;
    }

    public int getBitsPerValue() {
      return reader.getBitsPerValue();
    }

    public int size() {
      return reader.size();
    }
  }

}
