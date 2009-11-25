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

import org.apache.lucene.util.ArrayUtil;
import java.io.UnsupportedEncodingException;

/** Represents the UTF8 bytes[] for a term's text.  This is
 *  used when reading with the flex API, to avoid having to
 *  materialize full char[]. */
public final class TermRef {

  public byte[] bytes;
  public int offset;
  public int length;

  public TermRef() {
  }

  /**
   * @param text Well-formed unicode text, with no unpaired surrogates or U+FFFF.
   */
  public TermRef(String text) {
    copy(text);
  }

  // nocommit: we could do this w/ UnicodeUtil w/o requiring
  // allocation of new bytes[]?
  /**
   * @param text Well-formed unicode text, with no unpaired surrogates or U+FFFF.
   */
  public void copy(String text) {
    try {
      bytes = text.getBytes("UTF-8");
    } catch (UnsupportedEncodingException uee) {
      // should not happen:
      throw new RuntimeException("unable to encode to UTF-8");
    }
    offset = 0;
    length = bytes.length;
  }

  public int compareTerm(TermRef other) {
    final int minLength;
    if (length < other.length) {
      minLength = length;
    } else {
      minLength = other.length;
    }
    int upto = offset;
    int otherUpto = other.offset;
    final byte[] otherBytes = other.bytes;
    for(int i=0;i<minLength;i++) {
      // compare bytes as unsigned
      final int b1 = bytes[upto++]&0xff;
      final int b2 = otherBytes[otherUpto++]&0xff;
      final int diff =  b1-b2;
      if (diff != 0) {
        return diff;
      }
    }
    return length - other.length;
  }

  public boolean termEquals(TermRef other) {
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
    TermRef other = new TermRef();
    other.bytes = new byte[length];
    System.arraycopy(bytes, offset, other.bytes, 0, length);
    other.length = length;
    return other;
  }

  public boolean startsWith(TermRef other, int pos) {
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
  
  public boolean startsWith(TermRef other) {
    return startsWith(other, 0);
  }

  public boolean endsWith(TermRef other) {
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
    return this.termEquals((TermRef) other);
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
      sb.append(""+bytes[i]);
    }
    sb.append(']');
    return sb.toString();
  }

  public void copy(TermRef other) {
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
}