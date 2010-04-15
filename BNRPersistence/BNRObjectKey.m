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

BNRObjectKey BNRMakeKeyFromId(UInt32 rowId)
{
    return (BNRObjectKey) {
        NULL,
        CFSwapInt32HostToLittle(rowId)
    };
}
BNRObjectKey BNRMakeKeyFromBytes(const void *data, UInt32 length)
{
    if (data == NULL) {
        // We're probably making a copy of another ID key. At any rate,
        // we don't need to allocate any memory to copy an empty data field.
        return (BNRObjectKey) {
            NULL,
            length
        };
    } else if (length == sizeof(UInt32)) {
        // Pack the data into the length field.
        return (BNRObjectKey) {
            NULL,
            *((UInt32 *)data)
        };
    } else {
        BNRObjectKey key = {
            malloc(length),
            length
        };
        
        if (key.length) {
            memcpy((void *)key.data, data, length);
        }
        
        return key;        
    }
}

BNRObjectKey BNRMakeKeyFromBytesNoCopy(const void *data, UInt32 length)
{
    return (BNRObjectKey) {
        data,
        length
    };    
}

UInt32 BNRKeyHash(BNRObjectKey key)
{
    if (key.data == NULL) {
        return key.length;
    }
    
    return [[NSData dataWithBytesNoCopy:(void *)key.data length:key.length] hash];
    //    UInt32 hashcode = length;
    //    for (UInt32 count = length >> 2; count > 1; count--) {
    //        hashcode ^= ((const UInt32 *) data)[count] + count;
    //    }
    //    if (count) {
    //        for (int c = length & 3) {
    //            hashcode ^=  
    //        }
    //    }
}

BOOL BNREqualsKey(BNRObjectKey key1, BNRObjectKey key2)
{
    if (key1.data == key2.data && key1.data == NULL) {
        return key1.length == key2.length;
    } else if (key1.length != key2.length) {
        return NO;
    } else if (key1.data == key2.data) {
        return YES;
    } else {
        return memcmp(key1.data, key2.data, key1.length) == 0;
    }
}

UInt32 BNRKeyRowId(BNRObjectKey key)
{
    if (key.data != NULL) {
        // The key doesn't have a rowId. Pass back the null id.
        return 0;
    }
    
    return CFSwapInt32LittleToHost(key.length);
}

UInt32 BNRKeyLength(BNRObjectKey key)
{
    if (key.data == NULL) {
        return sizeof(UInt32);
    } else {
        return key.length;
    }
}

BNRObjectKey BNRCloneKey(BNRObjectKey key)
{
    return BNRMakeKeyFromBytes(key.data, key.length);
}

const void *BNRKeyData(BNRObjectKey *key)
{
    if (key->data == NULL) {
        return (const void *)(&key->length);
    } else {
        return key->data;
    }
}

void BNRFreeKey(BNRObjectKey key)
{
    if (key.data != NULL) {
        free((void *)key.data);
    }
}

BOOL BNRKeyIsNull(BNRObjectKey key)
{
    return key.length == 0 && key.data == NULL;
}
