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


#import "BNRStoreBackend.h"


@implementation BNRStoreBackend

#pragma mark Transaction support
- (BOOL)beginTransaction
{
    return NO;
}
- (BOOL)commitTransaction
{
    return NO;
}
- (BOOL)abortTransaction
{
    return NO;
}

- (BOOL)hasOpenTransaction
{
    return NO;
}


#pragma mark Writing changes

- (void)insertData:(BNRDataBuffer *)attNames
          forClass:(Class)c
            rowKey:(BNRObjectKey)key
{
    NSLog(@"insertData:forClass:rowID: not defined for %@", self);
}

- (void)deleteDataForClass:(Class)c
                    rowKey:(BNRObjectKey)key
{
    NSLog(@"deleteDataForClass:rowID: not defined for %@", self);
}

- (void)updateData:(BNRDataBuffer *)d 
          forClass:(Class)c 
            rowKey:(BNRObjectKey)key
{
    NSLog(@"updateData:forClass:rowID: not defined for %@", self);
}

#pragma mark Fetching

- (BNRDataBuffer *)dataForClass:(Class)c 
                         rowKey:(BNRObjectKey)key
{
    return nil;
}

- (BNRBackendCursor *)cursorForClass:(Class)c
{
    return nil;
}

- (void)close
{
    
}

@end
