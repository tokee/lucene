package org.apache.lucene.util;

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

import java.io.UnsupportedEncodingException;

/** Represents byte[], as a slice (offset + length) into an
 *  existing byte[]. */
public final class BytesRef {

  public byte[] bytes;
  public int offset;
  public int length;

  public BytesRef() {
  }

  /**
   * @param text Initialize the byte[] from the UTF8 bytes
   * for the provided Sring.  This must be well-formed
   * unicode text, with no unpaired surrogates or U+FFFF.
   */
  public BytesRef(String text) {
    copy(text);
  }

  public BytesRef(BytesRef other) {
    copy(other);
  }

  // nocommit: we could do this w/ UnicodeUtil w/o requiring
  // allocation of new bytes[]?
  /**
   * Copies the UTF8 bytes for this string.
   * 
   * @param text Must be well-formed unicode text, with no
   * unpaired surrogates or U+FFFF.
   */
  public void copy(String text) {
    // nocommit -- assert text has no unpaired surrogates??
    try {
      bytes = text.getBytes("UTF-8");
    } catch (UnsupportedEncodingException uee) {
      // should not happen:
      throw new RuntimeException("unable to encode to UTF-8");
    }
    offset = 0;
    length = bytes.length;
  }

  public boolean bytesEquals(BytesRef other) {
    if (length == other.length) {
      int upto = offset;
      int otherUpto = other.offset;
      final byte[] otherBytes = other.bytes;
      for(int i=0;i<length;i++) {
        if (bytes[upto++] != otherBytes[otherUpto++]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Object clone() {
    BytesRef other = new BytesRef();
    other.bytes = new byte[length];
    System.arraycopy(bytes, offset, other.bytes, 0, length);
    other.length = length;
    return other;
  }

  public boolean startsWith(BytesRef other, int pos) {
    // nocommit: maybe this one shouldn't be public...
    if (pos < 0 || length - pos < other.length) {
      return false;
    }
    int i = offset + pos;
    int j = other.offset;
    final int k = other.offset + other.length;
    
    while (j < k)
      if (bytes[i++] != other.bytes[j++])
        return false;
    
    return true;
  }
  
  public boolean startsWith(BytesRef other) {
    return startsWith(other, 0);
  }

  public boolean endsWith(BytesRef other) {
    return startsWith(other, length - other.length);   
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + hash(bytes);
    return result;
  }
  
  private int hash(byte a[]) {
    if (a == null) {
      return 0;
    }
    int result = 1;
    int upTo = offset;
    for(int i = 0; i < length; i++) {
      result = 31 * result + bytes[upTo++];
    }
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return this.bytesEquals((BytesRef) other);
  }

  @Override
  public String toString() {
    try {
      return new String(bytes, offset, length, "UTF-8");
    } catch (UnsupportedEncodingException uee) {
      // should not happen
      throw new RuntimeException(uee);
    }
  }

  public String toBytesString() {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    final int end = offset + length;
    for(int i=offset;i<end;i++) {
      if (i > offset) {
        sb.append(' ');
      }
      sb.append(Integer.toHexString(bytes[i]&0xff));
    }
    sb.append(']');
    return sb.toString();
  }

  private final String asUnicodeChar(char c) {
    return "U+" + Integer.toHexString(c);
  }

  // for debugging only -- this is slow
  public String toUnicodeString() {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    final String s = toString();
    for(int i=0;i<s.length();i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(asUnicodeChar(s.charAt(i)));
    }
    sb.append(']');
    return sb.toString();
  }

  public void copy(BytesRef other) {
    if (bytes == null) {
      bytes = new byte[other.length];
    } else {
      bytes = ArrayUtil.grow(bytes, other.length);
    }
    System.arraycopy(other.bytes, other.offset, bytes, 0, other.length);
    length = other.length;
    offset = 0;
  }

  public void grow(int newLength) {
    bytes = ArrayUtil.grow(bytes, newLength);
  }

  public abstract static class Comparator {
    abstract public int compare(BytesRef a, BytesRef b);
  }

  private final static Comparator utf8SortedAsUTF16SortOrder = new UTF8SortedAsUTF16Comparator();

  public static Comparator getUTF8SortedAsUTF16Comparator() {
    return utf8SortedAsUTF16SortOrder;
  }

  public static class UTF8SortedAsUTF16Comparator extends Comparator {
    public int compare(BytesRef a, BytesRef b) {

      final byte[] aBytes = a.bytes;
      int aUpto = a.offset;
      final byte[] bBytes = b.bytes;
      int bUpto = b.offset;
      
      final int aStop;
      if (a.length < b.length) {
        aStop = aUpto + a.length;
      } else {
        aStop = aUpto + b.length;
      }

      while(aUpto < aStop) {
        int aByte = aBytes[aUpto++] & 0xff;
        int bByte = bBytes[bUpto++] & 0xff;

        if (aByte != bByte) {

          // See http://icu-project.org/docs/papers/utf16_code_point_order.html#utf-8-in-utf-16-order

          // We know the terms are not equal, but, we may
          // have to carefully fixup the bytes at the
          // difference to match UTF16's sort order:
          if (aByte >= 0xee && bByte >= 0xee) {
            if ((aByte & 0xfe) == 0xee) {
              aByte += 0x10;
            }
            if ((bByte&0xfe) == 0xee) {
              bByte += 0x10;
            }
          }
          return aByte - bByte;
        }
      }

      // One is a prefix of the other, or, they are equal:
      return a.length - b.length;
    }
  }
}
