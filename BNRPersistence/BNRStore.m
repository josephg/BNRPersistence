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

#import "BNRStore.h"
#import "BNRStoreBackend.h"
#import "BNRStoredObject.h"
#import "BNRClassDictionary.h"
#import "BNRBackendCursor.h"
#import "BNRDataBuffer.h"
#import "BNRClassMetaData.h"
#import "BNRUniquingTable.h"
#import "BNRIndexManager.h"
#import "BNRDataBuffer+Encryption.h"

#ifndef PAGE_SIZE
#define PAGE_SIZE (4096)
#endif

@interface BNRStoredObject (BNRStoreFriend)

- (void)setHasContent:(BOOL)yn;
- (id)initWithStore:(BNRStore *)s
             rowKey:(BNRObjectKey)k
             buffer:(BNRDataBuffer *)buffer;

@end


@implementation BNRStore
@synthesize undoManager, indexManager, delegate, usesPerInstanceVersioning, encryptionKey;

- (id)init
{
    [super init];
    uniquingTable = [[BNRUniquingTable alloc] init];
    toBeInserted = [[NSMutableSet alloc] init];
    toBeDeleted = [[NSMutableSet alloc] init];
    toBeUpdated = [[NSMutableSet alloc] init];
    classMetaData = [[BNRClassDictionary alloc] init];
    usesPerInstanceVersioning = YES; // Adds an 8-bit number to every record, but enables versioning...
    return self;
}

- (void)setDelegate:(id <BNRStoreDelegate>)obj
{
    delegate = obj;
}

- (void)setUndoManager:(NSUndoManager *)ud
{
    [ud retain];
    [undoManager release];
    undoManager = ud;
}

- (void)makeEveryStoredObjectPerformSelector:(SEL)s
{
    [uniquingTable makeAllObjectsPerformSelector:s];
}


- (void)dissolveAllRelationships
{
    [self makeEveryStoredObjectPerformSelector:@selector(dissolveAllRelationships)];

}

- (void)dealloc
{
    NSLog(@"closing store");
    [uniquingTable release];
    [toBeInserted release];
    [toBeDeleted release];
    [toBeUpdated release];
    [indexManager close];
    [indexManager release];
    [backend release];
    [classMetaData release];
    [super dealloc];
}
    
- (void)addClass:(Class)c
{    
    // Put it in the first empty slot
    int classCount = 0;
    while (classes[classCount] != NULL) {
        classCount++;
    }
    classes[classCount] = c;
}

- (BOOL)decryptBuffer:(BNRDataBuffer *)buffer
              ofClass:(Class)c
               rowKey:(BNRObjectKey)key
{
    if (buffer == nil)
        return YES;
    
    BNRClassMetaData *metaData = [self metaDataForClass:c];
    
    UInt32 salt[2];
    memcpy(salt, [metaData encryptionKeySalt], 8);
    salt[1] = salt[1] ^ key.length;
    
    return [buffer decryptWithKey:encryptionKey salt:salt];
}
- (void)encryptBuffer:(BNRDataBuffer *)buffer
              ofClass:(Class)c
               rowKey:(BNRObjectKey)key
{
    UInt32 salt[2];
    memcpy(salt, [[self metaDataForClass:c] encryptionKeySalt], 8);
    salt[1] = salt[1] ^ key.length;
    [buffer encryptWithKey:encryptionKey salt:salt]; // does not encrypt if encryptionKey is empty.
}

#pragma mark Fetching

- (BNRStoredObject *)objectForClass:(Class)c 
                              rowID:(UInt32)n 
                       fetchContent:(BOOL)mustFetch
{
    return [self objectForClass:c
                         rowKey:BNRMakeKeyFromId(n)
                   fetchContent:mustFetch];
}

//- (BNRStoredObject *)objectForClass:(Class)c 
//                            rowName:(NSString *)key
//                       fetchContent:(BOOL)yn
//{
//    return [self objectForClass:c keyData:[key UTF8String] length:[key length] fetchContent:yn];
//}

- (BNRStoredObject *)objectForClass:(Class)c 
                             rowKey:(BNRObjectKey)key
                       fetchContent:(BOOL)mustFetch
{
    // Try to find it in the uniquing table
    BNRStoredObject *obj = [uniquingTable objectForClass:c rowKey:key];
    
    if (obj) {
        if (mustFetch && ![obj hasContent]) {
            BNRDataBuffer *const d = [backend dataForClass:c rowKey:key];
            [self decryptBuffer:d ofClass:c rowKey:key];
            if (usesPerInstanceVersioning) {
                [d consumeVersion];
            }
            [obj readContentFromBuffer:d];
            [obj setHasContent:YES];
        }
    } else {
        BNRDataBuffer *const d = mustFetch? [backend dataForClass:c rowKey:key] : nil;
        [self decryptBuffer:d ofClass:c rowKey:key];
        if (usesPerInstanceVersioning) {
            [d consumeVersion];
        }
        obj = [[[c alloc] initWithStore:self rowKey:key buffer:d] autorelease];
        [uniquingTable setObject:obj forClass:c rowKey:key];
    }
    return obj;
}


- (NSMutableArray *)allObjectsForClass:(Class)c
{
    // Fetch!
    BNRBackendCursor *const cursor = [backend cursorForClass:c];
    if (!cursor) {
        NSLog(@"No database for %@", NSStringFromClass(c));
        return nil;
    }
    NSMutableArray *const allObjects = [NSMutableArray array];
    BNRDataBuffer *const buffer = [[[BNRDataBuffer alloc]
                                    initWithCapacity:(UINT16_MAX + 1)]
                                   autorelease];

    BNRObjectKey rowKey;
    while (0 != BNRKeyRowId((rowKey = [cursor nextBuffer:buffer])))
    {
        if (kBNRMetadataRowID == BNRKeyRowId(rowKey)) continue;  // skip metadata

        // Get the next object.
        BNRStoredObject *storedObject = [self objectForClass:c
                                                      rowKey:rowKey
                                                fetchContent:NO];
        [allObjects addObject:storedObject];
        // Possibly read in its stored data.
        const BOOL hasUnsavedData = [toBeUpdated containsObject:storedObject];
        if (!hasUnsavedData) {
            [self decryptBuffer:buffer ofClass:c rowKey:rowKey];
            if (usesPerInstanceVersioning) {
                [buffer consumeVersion];
            }
            [storedObject readContentFromBuffer:buffer];
            [storedObject setHasContent:YES];
        }
     }
    return allObjects;
}

#if NS_BLOCKS_AVAILABLE
- (void)enumerateAllObjectsForClass:(Class)c usingBlock:(BNRStoredObjectIterBlock)iterBlock
{
    // Fetch!
    BNRBackendCursor *const cursor = [backend cursorForClass:c];
    if (!cursor) {
        NSLog(@"No database for %@", NSStringFromClass(c));
        return;
    }

    BNRDataBuffer *const buffer = [[[BNRDataBuffer alloc]
                                    initWithCapacity:(UINT16_MAX + 1)]
                                   autorelease];
    
    BNRObjectKey rowKey;
    while (0 != BNRKeyRowId((rowKey = [cursor nextBuffer:buffer])))
    {
        if (kBNRMetadataRowID == BNRKeyRowId(rowKey)) continue;  // skip metadata
        
        // Prevent our usage from building up while iterating over the objects:
        NSAutoreleasePool *pool = [[NSAutoreleasePool alloc] init];
        
        // Get the next object.
        BNRStoredObject *storedObject = [self objectForClass:c
                                                      rowKey:rowKey
                                                fetchContent:NO];
        // Possibly read in its stored data.
        const BOOL hasUnsavedData = [toBeUpdated containsObject:storedObject];
        if (!hasUnsavedData) {
            [self decryptBuffer:buffer ofClass:c rowKey:rowKey];
            if (usesPerInstanceVersioning) {
                [buffer consumeVersion];
            }
            [storedObject readContentFromBuffer:buffer];
            [storedObject setHasContent:YES];
        }
        
        BOOL stop = NO;
        iterBlock(rowKey, storedObject, &stop);
        
        [pool drain];
        
        if (stop)
            break;
    }
}
#endif

- (NSMutableArray *)objectsForClass:(Class)c
                       matchingText:(NSString *)toMatch
                             forKey:(NSString *)key
{
    if (!indexManager) {
        NSLog(@"No fulltext search without an index manager");
        return nil;
    }
    
    BNRObjectKey *resultKeys = NULL;
    
    UInt32 rowCount = [indexManager countOfRowsInClass:c 
                                          matchingText:toMatch
                                                forKey:key
                                                  list:&resultKeys];
    
    NSMutableArray *result = [NSMutableArray array];
    for (UInt32 i = 0; i < rowCount; i++) {
        BNRObjectKey rowKey = resultKeys[i];
        BNRStoredObject *obj = [self objectForClass:c 
                                             rowKey:rowKey
                                       fetchContent:NO];
        [result addObject:obj];
    }
    if (NULL != resultKeys) {
        free(resultKeys);
    }
    return result;
}

#pragma mark Insert, update, delete

- (BOOL)hasUnsavedChanges
{
    return [toBeDeleted count] || [toBeInserted count] || [toBeUpdated count];
}

- (void)insertObject:(BNRStoredObject *)obj
{
    [obj setStore:self];
    
    Class c = [obj class];
    
    // Should I really be giving this object
    // a row ID?
    if (![obj hasKey]) {
        UInt32 rowID = [self nextRowIDForClass:c];
        [obj setRowID:rowID];        
    }
    
    // Put it in the uniquing table
    [uniquingTable setObject:obj];

    if (undoManager) {
        [(BNRStore *)[undoManager prepareWithInvocationTarget:self] deleteObject:obj];
    }
    
    [self willChangeValueForKey:@"hasUnsavedChanges"];
    [toBeInserted addObject:obj];
    [toBeUpdated removeObject:obj];
    [toBeDeleted removeObject:obj];
    [self didChangeValueForKey:@"hasUnsavedChanges"];
    
    if (delegate) {
        [delegate store:self willInsertObject:obj];
    }
}

// This insert is used when undoing a delete. It frees the key after inserting, so be careful
// when using it in other contexts.
- (void)insertWithRowKey:(BNRObjectKey)rowKey
                   class:(Class)c
                snapshot:(BNRDataBuffer *)snap
{
    BNRStoredObject *obj = [self objectForClass:c
                                         rowKey:rowKey
                                   fetchContent:NO];
    if (usesPerInstanceVersioning) {
        [snap consumeVersion];
    }
    [obj readContentFromBuffer:snap];
    [self insertObject:obj];
    [obj release];
    BNRFreeKey(rowKey);
}

- (void)deleteObject:(BNRStoredObject *)obj
{
    // Prevents infinite recursion (see prepareForDelete)
    if ([toBeDeleted containsObject:obj]) {
        return;
    }
    
    [self willChangeValueForKey:@"hasUnsavedChanges"];
    
    if ([toBeInserted containsObject:obj]){
        [toBeInserted removeObject:obj];
    } else {
        [toBeDeleted addObject:obj];
    }
    
    // No need to insert or update deleted objects
    [toBeUpdated removeObject:obj];

    [self didChangeValueForKey:@"hasUnsavedChanges"];
    
    // Store away current values
    if (undoManager) {
        BNRDataBuffer *snapshot = [[BNRDataBuffer alloc]
                                   initWithCapacity:PAGE_SIZE];
        if (usesPerInstanceVersioning) {
            [snapshot writeVersionForObject:obj];
        }
        
        [obj writeContentToBuffer:snapshot];
        [snapshot resetCursor];
        //NSLog(@"snapshot for undo = %@", snapshot);

        BNRObjectKey rowKey = BNRCloneKey([obj rowKey]);
        Class c = [obj class];
        [[undoManager prepareWithInvocationTarget:self] insertWithRowKey:rowKey
                                                                   class:c
                                                                snapshot:snapshot];
        [snapshot release];
    }
    
    // objects implement their own delete rules - cascade, whatever
    [obj prepareForDelete];
    
    if (delegate) {
        [delegate store:self willDeleteObject:obj];
    }
}

- (void)updateObject:(BNRStoredObject *)obj withSnapshot:(BNRDataBuffer *)b
{
    // Store away current values
    if (undoManager) {
        BNRDataBuffer *snapshot = [[BNRDataBuffer alloc]
                                   initWithCapacity:PAGE_SIZE];
        if (usesPerInstanceVersioning) {
            [snapshot writeVersionForObject:obj];
        }
        
        [obj writeContentToBuffer:snapshot];
        [snapshot resetCursor];
        [[undoManager prepareWithInvocationTarget:self] updateObject:obj 
                                                        withSnapshot:snapshot];
        [snapshot release];
    }
    
    if (usesPerInstanceVersioning) {
        [b consumeVersion];
    }
    
    [obj readContentFromBuffer:b];
    if (delegate) {
        [delegate store:self didChangeObject:obj];
    }
    
    [toBeUpdated addObject:obj];
    if (delegate) {
        [delegate store:self willUpdateObject:obj];
    }
    
}

- (void)willUpdateObject:(BNRStoredObject *)obj
{
    if (undoManager) {
        BNRDataBuffer *snapshot = [[BNRDataBuffer alloc]
                                   initWithCapacity:PAGE_SIZE];
        if (usesPerInstanceVersioning) {
            [snapshot writeVersionForObject:obj];
        }
        
        [obj writeContentToBuffer:snapshot];
        [snapshot resetCursor];

        [[undoManager prepareWithInvocationTarget:self] updateObject:obj 
                                                        withSnapshot:snapshot];
        [snapshot release];
    }
    
    if (delegate) {
        [delegate store:self willUpdateObject:obj];
    }
    
    // No need to insert and update
    if (![toBeInserted containsObject:obj]) {
        [self willChangeValueForKey:@"hasUnsavedChanges"];
        [toBeUpdated addObject:obj];
        [self didChangeValueForKey:@"hasUnsavedChanges"];
    }
}

- (BOOL)saveChanges:(NSError **)errorPtr
{
    [self willChangeValueForKey:@"hasUnsavedChanges"];

    BNRDataBuffer *buffer = [[BNRDataBuffer alloc] initWithCapacity:65536];
    [backend beginTransaction];
    
    //NSLog(@"inserting %d objects", [toBeInserted count]);
     
    for (BNRStoredObject *obj in toBeInserted) {
        Class c = [obj class];
        BNRObjectKey rowKey = [obj rowKey];
        
        if (usesPerInstanceVersioning) {
            [buffer writeVersionForObject:obj];
        }        
        
        [obj writeContentToBuffer:buffer];
        
        [self encryptBuffer:buffer ofClass:c rowKey:rowKey]; // does not encrypt if encryptionKey is empty.
        
        [backend insertData:buffer
                   forClass:c
                     rowKey:rowKey];
        [buffer clearBuffer];
        
        if (indexManager) {
            [indexManager insertObjectInIndexes:obj];
        }
    }
    
    //NSLog(@"updating %d objects", [toBeUpdated count]);

    // Updates
    for (BNRStoredObject *obj in toBeUpdated) {
        Class c = [obj class];
        BNRObjectKey rowKey = [obj rowKey];
        if (usesPerInstanceVersioning) {
            [buffer writeVersionForObject:obj];
        }
        
        [obj writeContentToBuffer:buffer];
        
        [self encryptBuffer:buffer ofClass:c rowKey:rowKey]; // does not encrypt if encryptionKey is empty.
        
        [backend updateData:buffer
                   forClass:c
                     rowKey:rowKey];
        [buffer clearBuffer];
        
        // FIXME: updating all indexes is inefficient
        if (indexManager) {
            [indexManager updateObjectInIndexes:obj];
        }
        
    }
    // Deletes
    
    //NSLog(@"deleting %d objects", [toBeDeleted count]);
    for (BNRStoredObject *obj in toBeDeleted) {
        Class c = [obj class];
        BNRObjectKey rowKey = [obj rowKey];
        
        // Take it out of the uniquing table:
        // Should I remove it from the uniquingTable in deleteObject?
        [uniquingTable removeObjectForClass:c rowKey:rowKey];
        [obj setStore:nil];

        [backend deleteDataForClass:c
                             rowKey:rowKey];
        
        if (indexManager) {
            [indexManager deleteObjectFromIndexes:obj];
        }
        
    }
    
    // Write out class meta data
    // FIXME: things will be faster if you only 
    // save ones that have been changed
    int i = 0;
    Class c;
    while (c = classes[i]) {
        BNRClassMetaData *d = [classMetaData objectForClass:c];
        if (d) {
            [d writeContentToBuffer:buffer];
            //NSLog(@"Inserting %d bytes of meta data for %@", [buffer length], NSStringFromClass(c));

            [backend updateData:buffer
                       forClass:c
                         rowKey:BNRMakeKeyFromId(kBNRMetadataRowID)];
            [buffer clearBuffer];
        }
        i++;
    }
    [buffer release];
    
    BOOL successful = [backend commitTransaction];
    if (successful) {
        [toBeInserted removeAllObjects];
        [toBeUpdated removeAllObjects];
        [toBeDeleted removeAllObjects];
    } else {
        NSLog(@"Error: save was not successful");
        [backend abortTransaction];
    }
    
    [self didChangeValueForKey:@"hasUnsavedChanges"];        
    return successful;
}

#pragma mark Backend

- (BNRStoreBackend *)backend
{
    return backend;
}
- (void)setBackend:(BNRStoreBackend *)be
{
    [be retain];
    [backend release];
    backend = be;
}

#pragma mark Class meta data

- (BNRClassMetaData *)metaDataForClass:(Class)c
{
    BNRClassMetaData *md = [classMetaData objectForClass:c];
    if (!md) {
        md = [[BNRClassMetaData alloc] init];
        BNRDataBuffer *b;
        b = [backend dataForClass:c 
                           rowKey:BNRMakeKeyFromId(kBNRMetadataRowID)];
        
        // Did I find meta data in the database?
        if (b) {
            
            // Note: meta data data buffer is *not* prepended with a version #
            //NSLog(@"Read %d bytes of meta data for %@", [b length], NSStringFromClass(c));
            [md readContentFromBuffer:b];
            unsigned char classID = [md classID];
            classes[classID] = c;
        } else {
            unsigned char classID = 0;
            while (classes[classID] != c) {
                classID++;
                if (classID == 255) {
                    [NSException raise:@"Class not in classes"
                                format:@"Class %@ was not added to %@", NSStringFromClass(c), self];
                }
            }
            [md setClassID:classID];
        }
        [classMetaData setObject:md forClass:c];
        [md release];
    }
    return md;
}

- (unsigned)nextRowIDForClass:(Class)c
{
    BNRClassMetaData *md = [self metaDataForClass:c];
    return [md nextPrimaryKey];
}
- (unsigned char)versionForClass:(Class)c
{
    BNRClassMetaData *md = [self metaDataForClass:c];
    return [md versionNumber];
}
- (Class)classForClassID:(unsigned char)c
{
    return classes[c];
}
- (unsigned char)classIDForClass:(Class)c
{
    BNRClassMetaData *md = [self metaDataForClass:c];
    return [md classID];
}

- (NSString *)description
{
    return [NSString stringWithFormat:@"<BNRStore-%@ to insert:%d, to update %d, to delete %d>",
        backend, [toBeInserted count], [toBeUpdated count], [toBeDeleted count]];
}
@end
