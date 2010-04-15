// The MIT License
//
// Copyright (c) 2008 Big Nerd Ranch, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#import <Foundation/Foundation.h>
#import "BNRObjectKey.h"

@class BNRStoredObject;

struct UniquingListNode 
{
    BNRStoredObject *storedObject;
    struct UniquingListNode *next;
};

/*! 
 @class BNRUniquingTable
 @abstract Essentially a dictionary that maps (Class, BRNObjectKey) -> BNRStoredObject.  
 (Class, BRNObjectKey) pairs must be unique.  It is implemented as a hash table.
 */

// FIXME: this hash table has a fixed number of buckets.  It should grow as the need
// arises

@interface BNRUniquingTable : NSObject {
    UInt32 tableSize;
    struct UniquingListNode **table;
    
}
- (BNRStoredObject *)objectForClass:(Class)c rowKey:(BNRObjectKey)key;
- (void)setObject:(BNRStoredObject *)obj forClass:(Class)c rowKey:(BNRObjectKey)key;
- (void)setObject:(BNRStoredObject *)obj;
- (void)removeObjectForClass:(Class)c rowKey:(BNRObjectKey)key;

- (void)makeAllObjectsPerformSelector:(SEL)s;
@end
