package org.apache.lucene.index.codecs.standard;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.TermRef;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.util.ArrayUtil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Collection;
import java.io.IOException;

/**
 * Uses a simplistic format to record terms dict index
 * information.  Limititations:
 *
 *   - Index for all fields is loaded entirely into RAM up
 *     front 
 *   - Index is stored in RAM using shared byte[] that
 *     wastefully expand every term.  Using FST to share
 *     common prefix & suffix would save RAM.
 *   - Index is taken at regular numTerms (every 128 by
 *     default); might be better to do it by "net docFreqs"
 *     encountered, so that for spans of low-freq terms we
 *     take index less often.
 *
 * A better approach might be something similar to how
 * postings are encoded, w/ multi-level skips.  Ie, load all
 * terms index data into memory, as a single large compactly
 * encoded stream (eg delta bytes + delta offset).  Index
 * that w/ multi-level skipper.  Then to look up a term is
 * the equivalent binary search, using the skipper instead,
 * while data remains compressed in memory.
 */

import org.apache.lucene.index.IndexFileNames;

public class SimpleStandardTermsIndexReader extends StandardTermsIndexReader {

  final private int totalIndexInterval;
  private int indexDivisor;
  final private int indexInterval;

  final private IndexInput in;
  private volatile boolean indexLoaded;
  private final TermRef.Comparator termComp;

  final HashMap<FieldInfo,FieldIndexReader> fields = new HashMap<FieldInfo,FieldIndexReader>();

  public SimpleStandardTermsIndexReader(Directory dir, FieldInfos fieldInfos, String segment, int indexDivisor, TermRef.Comparator termComp)
    throws IOException {

    this.termComp = termComp;

    // nocommit -- why was this needed?
    String file = IndexFileNames.segmentFileName(segment, StandardCodec.TERMS_INDEX_EXTENSION);
    if (!dir.fileExists(file)) {
      indexInterval = 0;
      totalIndexInterval = 0;
      this.indexDivisor = indexDivisor;
      in = null;
      return;
    }
    IndexInput in = dir.openInput(file);
    
    boolean success = false;

    try {
      Codec.checkHeader(in, SimpleStandardTermsIndexWriter.CODEC_NAME, SimpleStandardTermsIndexWriter.VERSION_START);

      if (Codec.DEBUG) {
        System.out.println(" readDirStart @ " + in.getFilePointer());
      }

      final long dirOffset = in.readLong();

      indexInterval = in.readInt();
      this.indexDivisor = indexDivisor;

      if (indexDivisor == -1) {
        totalIndexInterval = indexInterval;
      } else {
        // In case terms index gets loaded, later, on demand
        totalIndexInterval = indexInterval * indexDivisor;
      }

      // Read directory
      in.seek(dirOffset);

      final int numFields = in.readInt();

      if (Codec.DEBUG) {
        System.out.println("sstir create seg=" + segment + " numFields=" + numFields + " dirStart=" + dirOffset);
      }

      for(int i=0;i<numFields;i++) {
        final int field = in.readInt();
        if (Codec.DEBUG) {
          System.out.println("  read field number=" + field);
        }
        final int numIndexTerms = in.readInt();
        final long indexStart = in.readLong();
        if (numIndexTerms > 0) {
          final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
          fields.put(fieldInfo, new FieldIndexReader(in, fieldInfo, numIndexTerms, indexStart));
        }
      }
      success = true;
    } finally {
      if (indexDivisor != -1) {
        in.close();
        this.in = null;
        if (success) {
          trimByteBlock();
          indexLoaded = true;
        }
      } else {
        this.in = in;
        // nocommit -- we should close if index gets read on demand?
      }
    }
  }

  /* Called when index is fully loaded.  We know we will use
   * no more bytes in the final byte[], so trim it down to
   * its actual usagee.  This substantially reduces memory
   * usage of SegmentReader searching a tiny segment. */
  private final void trimByteBlock() {
    if (blockOffset == 0) {
      // nocommit -- should not happen?  fields w/ no terms
      // are not written by STDW.  hmmm it does
      // happen... must explain why -- oh, could be only
      // on exception; I added only calling this on
      // success above
      //assert false;
      // nocommit -- hit AIOOBE here (blocks is length 0):
      if (blocks != null) {
        blocks[blockUpto] = null;
      }
      //System.out.println("Simple terms index consumed no bytes! blockCount=" + blocks.length);
    } else {
      byte[] last = new byte[blockOffset];
      System.arraycopy(blocks[blockUpto], 0, last, 0, blockOffset);
      blocks[blockUpto] = last;
    }
  }

  // nocommit -- we can record precisely how many bytes are
  // required during indexing, save that into file, and be
  // precise when we allocate the blocks; we even don't need
  // to use blocks anymore (though my still want to, to
  // prevent allocation failure due to mem fragmentation on
  // 32bit)

  // Fixed size byte blocks, to hold all term bytes; these
  // blocks are shared across fields
  private byte[][] blocks;
  int blockUpto;
  int blockOffset;

  // nocommit -- is this big enough, given max allowed term
  // size (measured in chars!!) ?
  // nocommit -- or, we could allocate one block way to big,
  // to accommodate such ridiculous terms
  private static final int BYTE_BLOCK_SHIFT = 15;
  private static final int BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT;
  private static final int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;

  private final class FieldIndexReader extends FieldReader {

    final private FieldInfo fieldInfo;

    private volatile CoreFieldIndex coreIndex;

    private final IndexInput in;

    private final long indexStart;

    private final int numIndexTerms;

    public FieldIndexReader(IndexInput in, FieldInfo fieldInfo, int numIndexTerms, long indexStart) throws IOException {

      this.fieldInfo = fieldInfo;
      this.in = in;
      this.indexStart = indexStart;
      this.numIndexTerms = numIndexTerms;

      // We still create the indexReader when indexDivisor
      // is -1, so that StandardTermsDictReader can call
      // isIndexTerm for each field:
      if (indexDivisor != -1) {

        if (Codec.DEBUG) {
          System.out.println("read index for field=" + fieldInfo.name + " numIndexTerms=" + numIndexTerms + " indexDivisor=" + indexDivisor + " indexFP=" + indexStart);
        }

        coreIndex = new CoreFieldIndex(indexStart,
                                       numIndexTerms);
      
      } else {
        if (Codec.DEBUG) {
          System.out.println("skip read index for field=" + fieldInfo.name + " numIndexTerms=" + numIndexTerms + " indexDivisor=" + indexDivisor);
        }
      }
    }

    public void loadTermsIndex() throws IOException {
      if (coreIndex == null) {
        coreIndex = new CoreFieldIndex(indexStart, numIndexTerms);
      }
    }

    @Override
    public boolean isIndexTerm(int position, int docFreq) {
      return position % totalIndexInterval == 0;
    }

    @Override
    public final void getIndexOffset(TermRef term, TermsIndexResult result) throws IOException {
      // You must call loadTermsIndex if you had specified -1 for indexDivisor
      if (coreIndex == null) {
        throw new IllegalStateException("terms index was not loaded");
      }
      coreIndex.getIndexOffset(term, result);
    }

    @Override
    public final void getIndexOffset(long ord, TermsIndexResult result) throws IOException {
      // You must call loadTermsIndex if you had specified -1 for indexDivisor
      if (coreIndex == null) {
        throw new IllegalStateException("terms index was not loaded");
      }
      coreIndex.getIndexOffset(ord, result);
    }

    private final class CoreFieldIndex {

      // TODO: used packed ints here
      // Pointer into terms dict file that we are indexing
      final long[] fileOffset;

      // TODO: used packed ints here
      // For each term, points to start of term's bytes within
      // block.
      // TODO: wasteful that this is always long; many terms
      // dict indexes obviously don't require so much address
      // space; since we know up front during indexing how
      // much space is needed we could pack this to the
      // precise # bits
      final long[] blockPointer;
    
      // Length of each term
      // nocommit -- this is length in bytes; is short
      // sufficient?  have to use negative space?
      // TODO: used packed ints here: we know max term
      // length; often its small
      final short[] termLength;

      final int numIndexTerms;

      CoreFieldIndex(long indexStart, int numIndexTerms) throws IOException {

        IndexInput clone = (IndexInput) in.clone();
        clone.seek(indexStart);

        if (indexDivisor == -1) {
          // Special case: we are being loaded inside
          // IndexWriter because a SegmentReader that at
          // first was opened for merging, is now being
          // opened to perform deletes or for an NRT reader

          // nocommit -- how to allow apps to indexDivisor
          // in this case?
          this.numIndexTerms = numIndexTerms;
        } else {
          this.numIndexTerms = 1+(numIndexTerms-1) / indexDivisor;
        }

        assert this.numIndexTerms  > 0: "numIndexTerms=" + numIndexTerms + " indexDivisor=" + indexDivisor;

        if (blocks == null) {
          blocks = new byte[1][];
          blocks[0] = new byte[BYTE_BLOCK_SIZE];
        }

        byte[] lastBlock = blocks[blockUpto];
        int lastBlockOffset = blockOffset;

        fileOffset = new long[this.numIndexTerms];
        blockPointer = new long[this.numIndexTerms];
        termLength = new short[this.numIndexTerms];
        
        final byte[] skipBytes;
        if (indexDivisor != 1) {
          // only need skipBytes (below) if we are not
          // loading all index terms
          skipBytes = new byte[128];
        } else {
          skipBytes = null;
        }

        int upto = 0;
        long pointer = 0;
      
        for(int i=0;i<numIndexTerms;i++) {
          final int start = clone.readVInt();
          final int suffix = clone.readVInt();
          final int thisTermLength = start + suffix;

          // nocommit -- verify this is in fact guaranteed by
          // DW -- we are talking bytes not chars here
          assert thisTermLength <= BYTE_BLOCK_SIZE;

          if (i%indexDivisor == 0) {
            // Keeper
            if (blockOffset + thisTermLength > BYTE_BLOCK_SIZE) {
              // New block
              final byte[] newBlock = new byte[BYTE_BLOCK_SIZE];
              if (blocks.length == blockUpto+1) {
                final int newSize = ArrayUtil.getNextSize(blockUpto+2);
                final byte[][] newBlocks = new byte[newSize][];
                System.arraycopy(blocks, 0, newBlocks, 0, blocks.length);
                blocks = newBlocks;
              }
              blockUpto++;
              blocks[blockUpto] = newBlock;
              blockOffset = 0;
            }

            //System.out.println("blockUpto=" + blockUpto + " blocks.length=" + blocks.length);
            final byte[] block = blocks[blockUpto];

            // Copy old prefix
            assert lastBlock != null || start == 0;
            assert block != null;
            System.arraycopy(lastBlock, lastBlockOffset, block, blockOffset, start);

            // Read new suffix
            clone.readBytes(block, blockOffset+start, suffix);

            // Advance file offset
            pointer += clone.readVLong();

            assert thisTermLength < Short.MAX_VALUE;

            termLength[upto] = (short) thisTermLength;
            fileOffset[upto] = pointer;
            blockPointer[upto] = blockUpto * BYTE_BLOCK_SIZE + blockOffset;

            /*
            TermRef tr = new TermRef();
            tr.bytes = blocks[blockUpto];
            tr.offset = blockOffset;
            tr.length = thisTermLength;

            //System.out.println("    read index term=" + new String(blocks[blockUpto], blockOffset, thisTermLength, "UTF-8") + " this=" + this + " bytes=" + block + " (vs=" + blocks[blockUpto] + ") offset=" + blockOffset);
            //System.out.println("    read index term=" + tr.toBytesString() + " this=" + this + " bytes=" + block + " (vs=" + blocks[blockUpto] + ") offset=" + blockOffset);
            */

            lastBlock = block;
            lastBlockOffset = blockOffset;
            blockOffset += thisTermLength;
            upto++;
          } else {
            // Skip bytes
            int toSkip = suffix;
            while(true) {
              if (toSkip > skipBytes.length) {
                clone.readBytes(skipBytes, 0, skipBytes.length);
                toSkip -= skipBytes.length;
              } else {
                clone.readBytes(skipBytes, 0, toSkip);
                break;
              }
            }

            // Advance file offset
            pointer += clone.readVLong();
          }
        }

        // nocommit: put in finally clause
        clone.close();

        assert upto == this.numIndexTerms;

        if (Codec.DEBUG) {
          System.out.println("  done read");
        }
      }

      public final void getIndexOffset(TermRef term, TermsIndexResult result) throws IOException {

        if (Codec.DEBUG) {
          System.out.println("getIndexOffset field=" + fieldInfo.name + " term=" + term + " indexLen = " + blockPointer.length + " numIndexTerms=" + fileOffset.length + " this=" + this + " numIndexedTerms=" + fileOffset.length);
        }

        int lo = 0;					  // binary search
        int hi = fileOffset.length - 1;

        while (hi >= lo) {
          int mid = (lo + hi) >> 1;

          final long loc = blockPointer[mid];
          result.term.bytes = blocks[(int) (loc >> BYTE_BLOCK_SHIFT)];
          result.term.offset = (int) (loc & BYTE_BLOCK_MASK);
          //System.out.println("  cycle mid=" + mid + " bytes=" + result.term.bytes + " offset=" + result.term.offset);
          result.term.length = termLength[mid];
          //System.out.println("    term=" + result.term);

          int delta = termComp.compare(term, result.term);
          if (delta < 0) {
            hi = mid - 1;
          } else if (delta > 0) {
            lo = mid + 1;
          } else {
            assert mid >= 0;
            result.position = mid*totalIndexInterval;
            result.offset = fileOffset[mid];
            return;
          }
        }
        if (hi < 0) {
          assert hi == -1;
          hi = 0;
        }

        final long loc = blockPointer[hi];
        result.term.bytes = blocks[(int) (loc >> BYTE_BLOCK_SHIFT)];
        result.term.offset = (int) (loc & BYTE_BLOCK_MASK);
        result.term.length = termLength[hi];

        result.position = hi*totalIndexInterval;
        result.offset = fileOffset[hi];
      }

      public final void getIndexOffset(long ord, TermsIndexResult result) throws IOException {
        int idx = (int) (ord / totalIndexInterval);
        // caller must ensure ord is in bounds
        assert idx < numIndexTerms;

        final long loc = blockPointer[idx];
        result.term.bytes = blocks[(int) (loc >> BYTE_BLOCK_SHIFT)];
        result.term.offset = (int) (loc & BYTE_BLOCK_MASK);
        result.term.length = termLength[idx];
        result.position = idx * totalIndexInterval;
        result.offset = fileOffset[idx];
      }
    }
  }

  @Override
  public void loadTermsIndex(int indexDivisor) throws IOException {
    if (!indexLoaded) {

      this.indexDivisor = indexDivisor;

      // mxx
      if (Codec.DEBUG) {
        System.out.println(Thread.currentThread().getName() + ": sstir: load coreIndex on demand");
      }

      Iterator<FieldIndexReader> it = fields.values().iterator();
      while(it.hasNext()) {
        it.next().loadTermsIndex();
      }
      indexLoaded = true;
      trimByteBlock();
    }
  }

  @Override
  public FieldReader getField(FieldInfo fieldInfo) {
    return fields.get(fieldInfo);
  }

  public static void files(Directory dir, SegmentInfo info, Collection<String> files) {
    files.add(IndexFileNames.segmentFileName(info.name, StandardCodec.TERMS_INDEX_EXTENSION));
  }

  public static void getIndexExtensions(Collection<String> extensions) {
    extensions.add(StandardCodec.TERMS_INDEX_EXTENSION);
  }

  @Override
  public void getExtensions(Collection<String> extensions) {
    getIndexExtensions(extensions);
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}
