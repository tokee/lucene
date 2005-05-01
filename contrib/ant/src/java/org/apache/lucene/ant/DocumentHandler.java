package org.apache.lucene.ant;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.document.Document;

import java.io.File;

/**
 *  Allows a class to act as a Lucene document handler
 *
 *@author     Erik Hatcher
 *@created    October 27, 2001
 */
public interface DocumentHandler {

    /**
     *  Gets the document attribute of the DocumentHandler object
     *
     *@param  file  Description of Parameter
     *@return       The document value
     */
    Document getDocument(File file)
            throws DocumentHandlerException;
}

