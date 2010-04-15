//
//  BNRUniquingTable.m
//  TCSpeedTest
//
//  Created by Aaron Hillegass on 1/28/10.
//  Copyright 2010 Big Nerd Ranch. All rights reserved.
//

#import "BNRUniquingTable.h"
#import "BNRStoredObject.h"

@interface BNRStoredObject (ClearingStore)
- (void)clearStore;
@end

@implementation BNRUniquingTable

- (id)init
{
    [super init];
    tableSize = 786433;
    //tableSize = 1572869;
    table = (struct UniquingListNode **)calloc(tableSize, sizeof(struct UniquingListNode *));
    return self;
}
- (void)dealloc
{
    for (UInt32 i = 0; i < tableSize; i++) {
        struct UniquingListNode *ptr = table[i];
        struct UniquingListNode *nextNode = NULL;
        while (ptr != NULL) {
            BNRStoredObject *currentObject = ptr->storedObject;
            nextNode = ptr->next;
            [currentObject clearStore];
            free(ptr);
            ptr = nextNode;
        }
    }
    free(table);
    [super dealloc];
}

- (void)setObject:(BNRStoredObject *)obj forClass:(Class)c rowKey:(BNRObjectKey)key
{
    NSAssert(!BNRKeyIsNull(key), @"key is null?!");
    
    UInt32 bucket = (BNRKeyHash(key) + (UInt64)c) % tableSize;
    struct UniquingListNode *ptr = table[bucket];
    struct UniquingListNode *lastPtr = NULL;
    while (ptr != NULL) {
        BNRStoredObject *currentObject = ptr->storedObject;
        if (BNREqualsKey(key, [currentObject rowKey])) {
            break;
        }
        lastPtr = ptr;
        ptr = ptr->next;
    }
    if (ptr) {
        ptr->storedObject = obj;
    } else {
        struct UniquingListNode *newNode = (struct UniquingListNode *)malloc(sizeof(struct UniquingListNode));
        newNode->storedObject = obj;
        newNode->next = NULL;
        if (lastPtr) {
            lastPtr->next = newNode;
        } else {
            table[bucket] = newNode;
        }
    }
}

- (void)setObject:(BNRStoredObject *)obj {
    [self setObject:obj forClass:[obj class] rowKey:[obj rowKey]];
}

- (BNRStoredObject *)objectForClass:(Class)c rowKey:(BNRObjectKey)key
{
    UInt32 bucket = (BNRKeyHash(key) + (UInt64)c) % tableSize;
    struct UniquingListNode *ptr = table[bucket];
    while (ptr != NULL) {
        BNRStoredObject *currentObject = ptr->storedObject;
        if (BNREqualsKey(key, [currentObject rowKey])) {
            return currentObject;
        }
        ptr = ptr->next;
    }
    return nil;
}

- (void)removeObjectForClass:(Class)c rowKey:(BNRObjectKey)key
{
    UInt32 bucket = (BNRKeyHash(key) + (UInt64)c) % tableSize;
    struct UniquingListNode *ptr = table[bucket];
    struct UniquingListNode *previousPtr = NULL;
    while (ptr != NULL) {
        BNRStoredObject *currentObject = ptr->storedObject;
        if (BNREqualsKey(key, [currentObject rowKey])) {
            break;
        }
        previousPtr = ptr;
        ptr = ptr->next;
    }
    if (ptr) {
        struct UniquingListNode *nextPtr = ptr->next;
        if (previousPtr) {
            previousPtr->next = nextPtr;
        } else {
            table[bucket] = nextPtr;
        }
        free(ptr);
    }    
}

- (void)makeAllObjectsPerformSelector:(SEL)s
{
    for (UInt32 i = 0; i < tableSize; i++) {
        struct UniquingListNode *ptr = table[i];
        while (ptr != NULL) {
            BNRStoredObject *currentObject = ptr->storedObject;
            [currentObject performSelector:s];
            ptr = ptr->next;
        }
    }
}

@end
