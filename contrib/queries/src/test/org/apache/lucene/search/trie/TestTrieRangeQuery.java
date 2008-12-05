package org.apache.lucene.search.trie;

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

import java.util.Random;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.RangeQuery;
import org.apache.lucene.util.LuceneTestCase;

public class TestTrieRangeQuery extends LuceneTestCase
{
	private static final long distance=66666;
	
	private static Random rnd=new Random();
	private static RAMDirectory directory;
	private static IndexSearcher searcher;
	static {
		try {
			directory = new RAMDirectory();
			IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(),
			true, MaxFieldLength.UNLIMITED);
			
			// Add a series of 10000 docs with increasing long values
			for (long l=0L; l<10000L; l++) {
				Document doc=new Document();
				// add fields, that have a distance to test general functionality
				TrieUtils.VARIANT_8BIT.addLongTrieCodedDocumentField(
					doc, "field8", distance*l, true /*index it*/, Field.Store.YES
				);
				TrieUtils.VARIANT_4BIT.addLongTrieCodedDocumentField(
					doc, "field4", distance*l, true /*index it*/, Field.Store.YES
				);
				TrieUtils.VARIANT_2BIT.addLongTrieCodedDocumentField(
					doc, "field2", distance*l, true /*index it*/, Field.Store.YES
				);
				// add ascending fields with a distance of 1 to test the correct splitting of range
				TrieUtils.VARIANT_8BIT.addLongTrieCodedDocumentField(
					doc, "ascfield8", l, true /*index it*/, Field.Store.NO
				);
				TrieUtils.VARIANT_4BIT.addLongTrieCodedDocumentField(
					doc, "ascfield4", l, true /*index it*/, Field.Store.NO
				);
				TrieUtils.VARIANT_2BIT.addLongTrieCodedDocumentField(
					doc, "ascfield2", l, true /*index it*/, Field.Store.NO
				);
				writer.addDocument(doc);
			}
		
			writer.close();
			searcher=new IndexSearcher(directory);			
		} catch (Exception e) {
			throw new Error(e);
		}
	}
	
	private void testRange(final TrieUtils variant) throws Exception {
		String field="field"+variant.TRIE_BITS;
		int count=3000;
		long lower=96666L, upper=lower + count*distance + 1234L;
		TrieRangeQuery q=new TrieRangeQuery(field, new Long(lower), new Long(upper), variant);
		TopDocs topDocs = searcher.search(q, null, 10000, Sort.INDEXORDER);
		System.out.println("Found "+q.getFilter().getLastNumberOfTerms()+" distinct terms in range for field '"+field+"'.");
		ScoreDoc[] sd = topDocs.scoreDocs;
		assertNotNull(sd);
		assertEquals("Score docs must match "+count+" docs, found "+sd.length+" docs", sd.length, count );
		Document doc=searcher.doc(sd[0].doc);
		assertEquals("First doc should be "+(2*distance), variant.trieCodedToLong(doc.get(field)), 2*distance );
		doc=searcher.doc(sd[sd.length-1].doc);
		assertEquals("Last doc should be "+((1+count)*distance), variant.trieCodedToLong(doc.get(field)), (1+count)*distance );
	}

	public void testRange_8bit() throws Exception {
		testRange(TrieUtils.VARIANT_8BIT);
	}
	
	public void testRange_4bit() throws Exception {
		testRange(TrieUtils.VARIANT_4BIT);
	}
	
	public void testRange_2bit() throws Exception {
		testRange(TrieUtils.VARIANT_2BIT);
	}
	
	private void testLeftOpenRange(final TrieUtils variant) throws Exception {
		String field="field"+variant.TRIE_BITS;
		int count=3000;
		long upper=(count-1)*distance + 1234L;
		TrieRangeQuery q=new TrieRangeQuery(field, null, new Long(upper), variant);
		TopDocs topDocs = searcher.search(q, null, 10000, Sort.INDEXORDER);
		System.out.println("Found "+q.getFilter().getLastNumberOfTerms()+" distinct terms in left open range for field '"+field+"'.");
		ScoreDoc[] sd = topDocs.scoreDocs;
		assertNotNull(sd);
		assertEquals("Score docs must match "+count+" docs, found "+sd.length+" docs", sd.length, count );
		Document doc=searcher.doc(sd[0].doc);
		assertEquals("First doc should be 0", variant.trieCodedToLong(doc.get(field)), 0L );
		doc=searcher.doc(sd[sd.length-1].doc);
		assertEquals("Last doc should be "+((count-1)*distance), variant.trieCodedToLong(doc.get(field)), (count-1)*distance );
	}
	
	public void testLeftOpenRange_8bit() throws Exception {
		testLeftOpenRange(TrieUtils.VARIANT_8BIT);
	}
	
	public void testLeftOpenRange_4bit() throws Exception {
		testLeftOpenRange(TrieUtils.VARIANT_4BIT);
	}
	
	public void testLeftOpenRange_2bit() throws Exception {
		testLeftOpenRange(TrieUtils.VARIANT_2BIT);
	}
	
	private void testRandomTrieAndClassicRangeQuery(final TrieUtils variant) throws Exception {
		String field="field"+variant.TRIE_BITS;
		// 50 random tests, the tests may also return 0 results, if min>max, but this is ok
		for (int i=0; i<50; i++) {
			long lower=(long)(rnd.nextDouble()*10000L*distance);
			long upper=(long)(rnd.nextDouble()*10000L*distance);
			TrieRangeQuery tq=new TrieRangeQuery(field, new Long(lower), new Long(upper), variant);
			RangeQuery cq=new RangeQuery(field, variant.longToTrieCoded(lower), variant.longToTrieCoded(upper), true, true);
			cq.setConstantScoreRewrite(true);
			TopDocs tTopDocs = searcher.search(tq, 1);
			TopDocs cTopDocs = searcher.search(cq, 1);
			assertEquals("Returned count for TrieRangeQuery and RangeQuery must be equal", tTopDocs.totalHits, cTopDocs.totalHits );
		}
	}
	
	public void testRandomTrieAndClassicRangeQuery_8bit() throws Exception {
		testRandomTrieAndClassicRangeQuery(TrieUtils.VARIANT_8BIT);
	}
	
	public void testRandomTrieAndClassicRangeQuery_4bit() throws Exception {
		testRandomTrieAndClassicRangeQuery(TrieUtils.VARIANT_4BIT);
	}
	
	public void testRandomTrieAndClassicRangeQuery_2bit() throws Exception {
		testRandomTrieAndClassicRangeQuery(TrieUtils.VARIANT_2BIT);
	}
	
	private void testRangeSplit(final TrieUtils variant) throws Exception {
		String field="ascfield"+variant.TRIE_BITS;
		// 50 random tests, the tests may also return 0 results, if min>max, but this is ok
		for (int i=0; i<50; i++) {
			long lower=(long)(rnd.nextDouble()*10000L);
			long upper=(long)(rnd.nextDouble()*10000L);
			if (lower>upper) {
				long a=lower; lower=upper; upper=a;
			}
			TrieRangeQuery tq=new TrieRangeQuery(field, new Long(lower), new Long(upper), variant);
			TopDocs tTopDocs = searcher.search(tq, 1);
			assertEquals("Returned count of range query must be equal to inclusive range length", tTopDocs.totalHits, upper-lower+1 );
		}
	}

	public void testRangeSplit_8bit() throws Exception {
		testRangeSplit(TrieUtils.VARIANT_8BIT);
	}
	
	public void testRangeSplit_4bit() throws Exception {
		testRangeSplit(TrieUtils.VARIANT_4BIT);
	}
	
	public void testRangeSplit_2bit() throws Exception {
		testRangeSplit(TrieUtils.VARIANT_2BIT);
	}
	
}