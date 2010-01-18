package org.apache.lucene.index.codecs.intblock;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** Naive int block API that writes vInts.  This is
 *  expected to give poor performance; it's really only for
 *  testing the pluggability.  One should typically use pfor instead. */

import java.io.IOException;

import org.apache.lucene.index.codecs.sep.IntIndexInput;
import org.apache.lucene.store.IndexInput;

/** Abstract base class that reads fixed-size blocks of ints
 *  from an IndexInput.  While this is a simple approach, a
 *  more performant approach would directly create an impl
 *  of IntIndexInput inside Directory.  Wrapping a generic
 *  IndexInput will likely cost performance.
 *
 * @lucene.experimental
 */
public abstract class FixedIntBlockIndexInput extends IntIndexInput {

  private IndexInput in;
  protected int blockSize;

  protected void init(final IndexInput in) throws IOException {
    this.in = in;
    blockSize = in.readVInt();
  }

  @Override
  public Reader reader() throws IOException {
    final int[] buffer = new int[blockSize];
    final IndexInput clone = (IndexInput) in.clone();
    // nocommit -- awkward
    return new Reader(clone, buffer, this.getBlockReader(clone, buffer));
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public Index index() {
    return new Index();
  }

  protected abstract BlockReader getBlockReader(IndexInput in, int[] buffer) throws IOException;

  public interface BlockReader {
    public void readBlock() throws IOException;
  }

  private static class Reader extends IntIndexInput.Reader {
    private final IndexInput in;

    protected final int[] pending;
    int upto;

    private boolean seekPending;
    private long pendingFP;
    private int pendingUpto;
    private long lastBlockFP;
    private final BlockReader blockReader;
    private final int blockSize;

    private final BulkReadResult result = new BulkReadResult();

    public Reader(final IndexInput in, final int[] pending, final BlockReader blockReader)
    throws IOException {
      this.in = in;
      this.pending = pending;
      this.blockSize = pending.length;
      result.buffer = pending;
      this.blockReader = blockReader;
      upto = blockSize;
    }

    void seek(final long fp, final int upto) {
      pendingFP = fp;
      pendingUpto = upto;
      seekPending = true;
    }

    private void maybeSeek() throws IOException {
      if (seekPending) {
        if (pendingFP != lastBlockFP) {
          // need new block
          in.seek(pendingFP);
          lastBlockFP = pendingFP;
          blockReader.readBlock();
        }
        upto = pendingUpto;
        seekPending = false;
      }
    }

    @Override
    public int next() throws IOException {
      this.maybeSeek();
      if (upto == blockSize) {
        lastBlockFP = in.getFilePointer();
        blockReader.readBlock();
        upto = 0;
      }

      return pending[upto++];
    }

    @Override
    public BulkReadResult read(final int[] buffer, final int count) throws IOException {
      this.maybeSeek();
      if (upto == blockSize) {
        blockReader.readBlock();
        upto = 0;
      }
      result.offset = upto;
      if (upto + count < blockSize) {
        result.len = count;
        upto += count;
      } else {
        result.len = blockSize - upto;
        upto = blockSize;
      }

      return result;
    }

    @Override
    public String descFilePointer() {
      return in.getFilePointer() + ":" + upto;
    }
  }

  private class Index extends IntIndexInput.Index {
    private long fp;
    private int upto;

    @Override
    public void read(final IndexInput indexIn, final boolean absolute) throws IOException {
      if (absolute) {
        fp = indexIn.readVLong();
        upto = indexIn.readVInt();
      } else {
        final long delta = indexIn.readVLong();
        if (delta == 0) {
          // same block
          upto += indexIn.readVInt();
        } else {
          // new block
          fp += delta;
          upto = indexIn.readVInt();
        }
      }
      assert upto < blockSize;
    }

    @Override
    public void seek(final IntIndexInput.Reader other) throws IOException {
      ((Reader) other).seek(fp, upto);
    }

    @Override
    public void set(final IntIndexInput.Index other) {
      final Index idx = (Index) other;
      fp = idx.fp;
      upto = idx.upto;
    }

    public class State extends IndexState {
      long fp;
      int upto;
    }

    // nocommit handle with set and/or clone?
    @Override
    public IndexState captureState() {
      final State state = new State();
      state.fp = fp;
      state.upto = upto;
      return state;
    }

    // nocommit handle with set and/or clone?
    @Override
    public void setState(final IndexState state) {
      final State iState = (State) state;
      this.fp = iState.fp;
      this.upto = iState.upto;

    }
  }
}
