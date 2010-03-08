package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.BytesRef;

final class TermsHashPerField extends InvertedDocConsumerPerField {

  final TermsHashConsumerPerField consumer;
  final TermsHashPerField nextPerField;
  final TermsHashPerThread perThread;
  final DocumentsWriter.DocState docState;
  final FieldInvertState fieldState;
  TermAttribute termAtt;

  // Copied from our perThread
  final IntBlockPool intPool;
  final ByteBlockPool bytePool;
  final ByteBlockPool termBytePool;

  final int streamCount;
  final int numPostingInt;

  final FieldInfo fieldInfo;

  boolean postingsCompacted;
  int numPostings;
  private int postingsHashSize = 4;
  private int postingsHashHalfSize = postingsHashSize/2;
  private int postingsHashMask = postingsHashSize-1;
  private RawPostingList[] postingsHash = new RawPostingList[postingsHashSize];
  private RawPostingList p;
  private final BytesRef utf8;
  private Comparator<BytesRef> termComp;

  public TermsHashPerField(DocInverterPerField docInverterPerField, final TermsHashPerThread perThread, final TermsHashPerThread nextPerThread, final FieldInfo fieldInfo) {
    this.perThread = perThread;
    intPool = perThread.intPool;
    bytePool = perThread.bytePool;
    termBytePool = perThread.termBytePool;
    docState = perThread.docState;
    fieldState = docInverterPerField.fieldState;
    this.consumer = perThread.consumer.addField(this, fieldInfo);

    streamCount = consumer.getStreamCount();
    numPostingInt = 2*streamCount;
    utf8 = perThread.utf8;
    this.fieldInfo = fieldInfo;
    if (nextPerThread != null)
      nextPerField = (TermsHashPerField) nextPerThread.addField(docInverterPerField, fieldInfo);
    else
      nextPerField = null;
  }

  void shrinkHash(int targetSize) {
    assert postingsCompacted || numPostings == 0;

    // Cannot use ArrayUtil.shrink because we require power
    // of 2:
    int newSize = postingsHash.length;
    while(newSize >= 8 && newSize/4 > targetSize) {
      newSize /= 2;
    }

    if (newSize != postingsHash.length) {
      postingsHash = new RawPostingList[newSize];
      postingsHashSize = newSize;
      postingsHashHalfSize = newSize/2;
      postingsHashMask = newSize-1;
    }
  }

  public void reset() {
    if (!postingsCompacted)
      compactPostings();
    assert numPostings <= postingsHash.length;
    if (numPostings > 0) {
      perThread.termsHash.recyclePostings(postingsHash, numPostings);
      Arrays.fill(postingsHash, 0, numPostings, null);
      numPostings = 0;
    }
    postingsCompacted = false;
    if (nextPerField != null)
      nextPerField.reset();
  }

  @Override
  synchronized public void abort() {
    reset();
    if (nextPerField != null)
      nextPerField.abort();
  }

  public void initReader(ByteSliceReader reader, RawPostingList p, int stream) {
    assert stream < streamCount;
    final int[] ints = intPool.buffers[p.intStart >> DocumentsWriter.INT_BLOCK_SHIFT];
    final int upto = p.intStart & DocumentsWriter.INT_BLOCK_MASK;
    reader.init(bytePool,
                p.byteStart+stream*ByteBlockPool.FIRST_LEVEL_SIZE,
                ints[upto+stream]);
  }

  private synchronized void compactPostings() {
    int upto = 0;
    for(int i=0;i<postingsHashSize;i++) {
      if (postingsHash[i] != null) {
        if (upto < i) {
          postingsHash[upto] = postingsHash[i];
          postingsHash[i] = null;
        }
        upto++;
      }
    }

    assert upto == numPostings;
    postingsCompacted = true;
  }

  /** Collapse the hash table & sort in-place. */
  public RawPostingList[] sortPostings(Comparator<BytesRef> termComp) {
    this.termComp = termComp;
    compactPostings();
    quickSort(postingsHash, 0, numPostings-1);
    return postingsHash;
  }

  void quickSort(RawPostingList[] postings, int lo, int hi) {
    if (lo >= hi)
      return;
    else if (hi == 1+lo) {
      if (comparePostings(postings[lo], postings[hi]) > 0) {
        final RawPostingList tmp = postings[lo];
        postings[lo] = postings[hi];
        postings[hi] = tmp;
      }
      return;
    }

    int mid = (lo + hi) >>> 1;

    if (comparePostings(postings[lo], postings[mid]) > 0) {
      RawPostingList tmp = postings[lo];
      postings[lo] = postings[mid];
      postings[mid] = tmp;
    }

    if (comparePostings(postings[mid], postings[hi]) > 0) {
      RawPostingList tmp = postings[mid];
      postings[mid] = postings[hi];
      postings[hi] = tmp;

      if (comparePostings(postings[lo], postings[mid]) > 0) {
        RawPostingList tmp2 = postings[lo];
        postings[lo] = postings[mid];
        postings[mid] = tmp2;
      }
    }

    int left = lo + 1;
    int right = hi - 1;

    if (left >= right)
      return;

    RawPostingList partition = postings[mid];

    for (; ;) {
      while (comparePostings(postings[right], partition) > 0)
        --right;

      while (left < right && comparePostings(postings[left], partition) <= 0)
        ++left;

      if (left < right) {
        RawPostingList tmp = postings[left];
        postings[left] = postings[right];
        postings[right] = tmp;
        --right;
      } else {
        break;
      }
    }

    quickSort(postings, lo, left);
    quickSort(postings, left + 1, hi);
  }

  /** Compares term text for two Posting instance and
   *  returns -1 if p1 < p2; 1 if p1 > p2; else 0. */
  int comparePostings(RawPostingList p1, RawPostingList p2) {

    if (p1 == p2) {
      // Our quicksort does this, eg during partition
      return 0;
    }

    termBytePool.setBytesRef(perThread.tr1, p1.textStart);
    termBytePool.setBytesRef(perThread.tr2, p2.textStart);

    return termComp.compare(perThread.tr1, perThread.tr2);
  }

  /** Test whether the text for current RawPostingList p equals
   *  current tokenText in utf8. */
  private boolean postingEquals() {

    final byte[] text = termBytePool.buffers[p.textStart >> DocumentsWriter.BYTE_BLOCK_SHIFT];
    assert text != null;
    int pos = p.textStart & DocumentsWriter.BYTE_BLOCK_MASK;
    
    final int len;
    if ((text[pos] & 0x80) == 0) {
      // length is 1 byte
      len = text[pos];
      pos += 1;
    } else {
      // length is 2 bytes
      len = (text[pos]&0x7f) + ((text[pos+1]&0xff)<<7);
      pos += 2;
    }

    if (len == utf8.length) {
      final byte[] utf8Bytes = utf8.bytes;
      for(int tokenPos=0;tokenPos<utf8.length;pos++,tokenPos++) {
        if (utf8Bytes[tokenPos] != text[pos]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }
  
  private boolean doCall;
  private boolean doNextCall;

  @Override
  void start(Fieldable f) {
    termAtt = fieldState.attributeSource.addAttribute(TermAttribute.class);
    consumer.start(f);
    if (nextPerField != null) {
      nextPerField.start(f);
    }
  }
  
  @Override
  boolean start(Fieldable[] fields, int count) throws IOException {
    doCall = consumer.start(fields, count);
    if (nextPerField != null)
      doNextCall = nextPerField.start(fields, count);
    return doCall || doNextCall;
  }

  // Secondary entry point (for 2nd & subsequent TermsHash),
  // because token text has already been "interned" into
  // textStart, so we hash by textStart
  public void add(int textStart) throws IOException {

    int code = textStart;

    int hashPos = code & postingsHashMask;

    assert !postingsCompacted;

    // Locate RawPostingList in hash
    p = postingsHash[hashPos];

    if (p != null && p.textStart != textStart) {
      // Conflict: keep searching different locations in
      // the hash table.
      final int inc = ((code>>8)+code)|1;
      do {
        code += inc;
        hashPos = code & postingsHashMask;
        p = postingsHash[hashPos];
      } while (p != null && p.textStart != textStart);
    }

    if (p == null) {

      // First time we are seeing this token since we last
      // flushed the hash.

      // Refill?
      if (0 == perThread.freePostingsCount)
        perThread.morePostings();

      // Pull next free RawPostingList from free list
      p = perThread.freePostings[--perThread.freePostingsCount];
      assert p != null;

      p.textStart = textStart;
          
      assert postingsHash[hashPos] == null;
      postingsHash[hashPos] = p;
      numPostings++;

      if (numPostings == postingsHashHalfSize)
        rehashPostings(2*postingsHashSize);

      // Init stream slices
      if (numPostingInt + intPool.intUpto > DocumentsWriter.INT_BLOCK_SIZE)
        intPool.nextBuffer();

      if (DocumentsWriter.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt*ByteBlockPool.FIRST_LEVEL_SIZE)
        bytePool.nextBuffer();

      intUptos = intPool.buffer;
      intUptoStart = intPool.intUpto;
      intPool.intUpto += streamCount;

      p.intStart = intUptoStart + intPool.intOffset;

      for(int i=0;i<streamCount;i++) {
        final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
        intUptos[intUptoStart+i] = upto + bytePool.byteOffset;
      }
      p.byteStart = intUptos[intUptoStart];

      consumer.newTerm(p);

    } else {
      intUptos = intPool.buffers[p.intStart >> DocumentsWriter.INT_BLOCK_SHIFT];
      intUptoStart = p.intStart & DocumentsWriter.INT_BLOCK_MASK;
      consumer.addTerm(p);
    }
  }

  // Primary entry point (for first TermsHash)
  @Override
  void add() throws IOException {

    assert !postingsCompacted;

    // We are first in the chain so we must "intern" the
    // term text into textStart address

    // Get the text of this term.
    final char[] tokenText = termAtt.termBuffer();
    final int tokenTextLen = termAtt.termLength();
    
    //System.out.println("\nfield=" + fieldInfo.name + " add text=" + new String(tokenText, 0, tokenTextLen) + " len=" + tokenTextLen);
    
    int code = UnicodeUtil.UTF16toUTF8WithHash(tokenText, 0, tokenTextLen, utf8);

    int hashPos = code & postingsHashMask;

    // Locate RawPostingList in hash
    p = postingsHash[hashPos];

    if (p != null && !postingEquals()) {
      // Conflict: keep searching different locations in
      // the hash table.
      final int inc = ((code>>8)+code)|1;
      do {
        code += inc;
        hashPos = code & postingsHashMask;
        p = postingsHash[hashPos];
      } while (p != null && !postingEquals());
    }

    if (p == null) {

      //System.out.println("  not seen yet");

      // First time we are seeing this token since we last
      // flushed the hash.
      final int textLen2 = 2+utf8.length;
      if (textLen2 + bytePool.byteUpto > DocumentsWriter.BYTE_BLOCK_SIZE) {
        // Not enough room in current block

        if (utf8.length > DocumentsWriter.MAX_TERM_LENGTH_UTF8) {
          // Just skip this term, to remain as robust as
          // possible during indexing.  A TokenFilter
          // can be inserted into the analyzer chain if
          // other behavior is wanted (pruning the term
          // to a prefix, throwing an exception, etc).
          if (docState.maxTermPrefix == null) {
            docState.maxTermPrefix = new String(tokenText, 0, 30);
          }

          consumer.skippingLongTerm();
          return;
        }
        bytePool.nextBuffer();
      }

      // Refill?
      if (0 == perThread.freePostingsCount) {
        perThread.morePostings();
      }

      // Pull next free RawPostingList from free list
      p = perThread.freePostings[--perThread.freePostingsCount];
      assert p != null;

      final byte[] text = bytePool.buffer;
      final int textUpto = bytePool.byteUpto;
      p.textStart = textUpto + bytePool.byteOffset;

      // We first encode the length, followed by the UTF8
      // bytes.  Length is encoded as vInt, but will consume
      // 1 or 2 bytes at most (we reject too-long terms,
      // above).

      // encode length @ start of bytes
      if (utf8.length < 128) {
        // 1 byte to store length
        text[textUpto] = (byte) utf8.length;
        bytePool.byteUpto += utf8.length + 1;
        System.arraycopy(utf8.bytes, 0, text, textUpto+1, utf8.length);
      } else {
        // 2 byte to store length
        text[textUpto] = (byte) (0x80 | (utf8.length & 0x7f));
        text[textUpto+1] = (byte) ((utf8.length>>7) & 0xff);
        bytePool.byteUpto += utf8.length + 2;
        System.arraycopy(utf8.bytes, 0, text, textUpto+2, utf8.length);
      }

      assert postingsHash[hashPos] == null;
      postingsHash[hashPos] = p;
      numPostings++;

      if (numPostings == postingsHashHalfSize) {
        rehashPostings(2*postingsHashSize);
      }

      // Init stream slices
      if (numPostingInt + intPool.intUpto > DocumentsWriter.INT_BLOCK_SIZE) {
        intPool.nextBuffer();
      }

      if (DocumentsWriter.BYTE_BLOCK_SIZE - bytePool.byteUpto < numPostingInt*ByteBlockPool.FIRST_LEVEL_SIZE) {
        bytePool.nextBuffer();
      }

      intUptos = intPool.buffer;
      intUptoStart = intPool.intUpto;
      intPool.intUpto += streamCount;

      p.intStart = intUptoStart + intPool.intOffset;

      for(int i=0;i<streamCount;i++) {
        final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
        intUptos[intUptoStart+i] = upto + bytePool.byteOffset;
      }
      p.byteStart = intUptos[intUptoStart];

      consumer.newTerm(p);

    } else {
      // System.out.println("  already seen");
      intUptos = intPool.buffers[p.intStart >> DocumentsWriter.INT_BLOCK_SHIFT];
      intUptoStart = p.intStart & DocumentsWriter.INT_BLOCK_MASK;
      consumer.addTerm(p);
    }

    if (doNextCall)
      nextPerField.add(p.textStart);
  }

  int[] intUptos;
  int intUptoStart;

  void writeByte(int stream, byte b) {
    int upto = intUptos[intUptoStart+stream];
    byte[] bytes = bytePool.buffers[upto >> DocumentsWriter.BYTE_BLOCK_SHIFT];
    assert bytes != null;
    int offset = upto & DocumentsWriter.BYTE_BLOCK_MASK;
    if (bytes[offset] != 0) {
      // End of slice; allocate a new one
      offset = bytePool.allocSlice(bytes, offset);
      bytes = bytePool.buffer;
      intUptos[intUptoStart+stream] = offset + bytePool.byteOffset;
    }
    bytes[offset] = b;
    (intUptos[intUptoStart+stream])++;
  }

  public void writeBytes(int stream, byte[] b, int offset, int len) {
    // TODO: optimize
    final int end = offset + len;
    for(int i=offset;i<end;i++)
      writeByte(stream, b[i]);
  }

  void writeVInt(int stream, int i) {
    assert stream < streamCount;
    while ((i & ~0x7F) != 0) {
      writeByte(stream, (byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte(stream, (byte) i);
  }

  @Override
  void finish() throws IOException {
    consumer.finish();
    if (nextPerField != null)
      nextPerField.finish();
  }

  /** Called when postings hash is too small (> 50%
   *  occupied) or too large (< 20% occupied). */
  void rehashPostings(final int newSize) {

    final int newMask = newSize-1;

    //System.out.println("  rehash");
    RawPostingList[] newHash = new RawPostingList[newSize];
    for(int i=0;i<postingsHashSize;i++) {
      RawPostingList p0 = postingsHash[i];
      if (p0 != null) {
        int code;
        if (perThread.primary) {
          final int start = p0.textStart & DocumentsWriter.BYTE_BLOCK_MASK;
          final byte[] text = bytePool.buffers[p0.textStart >> DocumentsWriter.BYTE_BLOCK_SHIFT];
          code = 0;

          final int len;
          int pos;
          if ((text[start] & 0x80) == 0) {
            // length is 1 byte
            len = text[start];
            pos = start+1;
          } else {
            len = (text[start]&0x7f) + ((text[start+1]&0xff)<<7);
            pos = start+2;
          }
          //System.out.println("    term=" + bytePool.setBytesRef(new BytesRef(), p0.textStart).toBytesString());

          final int endPos = pos+len;
          while(pos < endPos) {
            code = (code*31) + text[pos++];
          }
        } else {
          code = p0.textStart;
        }

        int hashPos = code & newMask;
        assert hashPos >= 0;
        if (newHash[hashPos] != null) {
          final int inc = ((code>>8)+code)|1;
          do {
            code += inc;
            hashPos = code & newMask;
          } while (newHash[hashPos] != null);
        }
        newHash[hashPos] = p0;
      }
    }

    postingsHashMask = newMask;
    postingsHash = newHash;
    postingsHashSize = newSize;
    postingsHashHalfSize = newSize >> 1;
  }
}
