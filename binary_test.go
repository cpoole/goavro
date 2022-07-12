// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/mohae/deepcopy"
)

var morePositiveThanMaxBlockCount, morePositiveThanMaxBlockSize, moreNegativeThanMaxBlockCount, mostNegativeBlockCount []byte

func init() {
	c, err := NewCodec(`"long"`)
	if err != nil {
		panic(err)
	}

	morePositiveThanMaxBlockCount, err = c.BinaryFromNative(nil, (MaxBlockCount + 1))
	if err != nil {
		panic(err)
	}

	morePositiveThanMaxBlockSize, err = c.BinaryFromNative(nil, (MaxBlockSize + 1))
	if err != nil {
		panic(err)
	}

	moreNegativeThanMaxBlockCount, err = c.BinaryFromNative(nil, -(MaxBlockCount + 1))
	if err != nil {
		panic(err)
	}

	mostNegativeBlockCount, err = c.BinaryFromNative(nil, int64(math.MinInt64))
	if err != nil {
		panic(err)
	}
}

func testBinaryDecodeFail(t *testing.T, schema string, buf []byte, errorMessage string) {
	t.Helper()
	c, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	value, newBuffer, err := c.NativeFromBinary(buf)
	ensureError(t, err, errorMessage)
	if value != nil {
		t.Errorf("GOT: %v; WANT: %v", value, nil)
	}
	if !bytes.Equal(buf, newBuffer) {
		t.Errorf("GOT: %v; WANT: %v", newBuffer, buf)
	}
}

func testBinaryEncodeFail(t *testing.T, schema string, datum interface{}, errorMessage string) {
	t.Helper()
	c, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := c.BinaryFromNative(nil, datum)
	ensureError(t, err, errorMessage)
	if buf != nil {
		t.Errorf("GOT: %v; WANT: %v", buf, nil)
	}
}

func testBinaryEncodeFailBadDatumType(t *testing.T, schema string, datum interface{}) {
	t.Helper()
	testBinaryEncodeFail(t, schema, datum, "received: ")
}

func testBinaryDecodeFailShortBuffer(t *testing.T, schema string, buf []byte) {
	t.Helper()
	testBinaryDecodeFail(t, schema, buf, "short buffer")
}

func testBinaryDecodePass(t *testing.T, schema string, datum interface{}, encoded []byte) {
	t.Helper()
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatal(err)
	}

	value, remaining, err := codec.NativeFromBinary(encoded)
	if err != nil {
		t.Fatalf("schema: %s; %s", schema, err)
	}

	// remaining ought to be empty because there is nothing remaining to be
	// decoded
	if actual, expected := len(remaining), 0; actual != expected {
		t.Errorf("schema: %s; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, actual, expected)
	}

	datumCopy := deepcopy.Copy(datum)

	if reflect.DeepEqual(value, datumCopy) {
		return
	}

	actual := fmt.Sprintf("%v", value)

	if value != nil {
		if reflect.TypeOf(value).Kind() == reflect.Ptr {
			var concreteValue interface{}
			if reflect.ValueOf(value).IsNil() {
				concreteValue = nil
			} else {
				concreteValue = reflect.Indirect(reflect.ValueOf(value)).Interface()
			}

			actual = fmt.Sprintf("%v", concreteValue)
		} else if reflect.TypeOf(value).Kind() == reflect.Map {
			concreteValue := make(map[string]interface{})
			for k, v := range value.(map[string]interface{}) {
				if v != nil && reflect.TypeOf(v).Kind() == reflect.Ptr {
					concreteValue[k] = reflect.Indirect(reflect.ValueOf(v)).Interface()
				} else {
					concreteValue[k] = v
				}
			}
			actual = fmt.Sprintf("%v", concreteValue)
		}
	}

	var concreteDatum interface{}

	if datumCopy == nil {
		concreteDatum = nil
	} else if reflect.TypeOf(datumCopy).Kind() == reflect.Ptr {
		if reflect.ValueOf(datumCopy).IsNil() {
			concreteDatum = nil
		} else {
			concreteDatum = reflect.Indirect(reflect.ValueOf(datumCopy)).Interface()
		}
	} else if reflect.TypeOf(datumCopy).Kind() == reflect.Map {
		// for maps we must iterate through the keys un unwrap the pointer values to perform a comparison
		unwrapped := make(map[string]interface{})
		for k, v := range datumCopy.(map[string]interface{}) {
			if v != nil && reflect.TypeOf(v).Kind() == reflect.Ptr {
				unwrapped[k] = reflect.Indirect(reflect.ValueOf(v)).Interface()
			} else {
				unwrapped[k] = v
			}
		}
		concreteDatum = unwrapped

	} else {
		concreteDatum = reflect.Indirect(reflect.ValueOf(datumCopy)).Interface()
	}

	expected := fmt.Sprintf("%v", concreteDatum)

	enumType, ok := concreteDatum.(avroEnum)
	if ok {
		expected = enumType.Str()
	}

	if actual != expected {
		// this is silly - but for certain types - specifically the logical binary types (math/big.Rat, etc)
		// deepcopy does not work and we end up with false false

		originalExpected := fmt.Sprintf("%v", datum)

		if actual != originalExpected {
			t.Errorf("schema: %s; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, actual, expected)
		} else {
			return
		}

		expectedBytes, err := json.Marshal(concreteDatum)
		if err != nil {
			t.Error(err)
		}

		actualBytes, err := json.Marshal(value)
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(actualBytes, expectedBytes) {
			t.Errorf("schema: %s; Datum: %v; Actual: %#v; Expected: %#v", schema, concreteDatum, actual, expected)
		}
	}
}

func testBinaryEncodePass(t *testing.T, schema string, datum interface{}, expected []byte) {
	t.Helper()
	codec, err := NewCodec(schema)
	if err != nil {
		t.Fatalf("Schma: %q %s", schema, err)
	}

	actual, err := codec.BinaryFromNative(nil, datum)
	if err != nil {
		t.Fatalf("schema: %s; Datum: %v; %s", schema, datum, err)
	}
	if !bytes.Equal(actual, expected) {
		t.Errorf("schema: %s; Datum: %v; Actual: %#v; Expected: %#v", schema, datum, actual, expected)
	}
}

// testBinaryCodecPass does a bi-directional codec check, by encoding datum to
// bytes, then decoding bytes back to datum.
func testBinaryCodecPass(t *testing.T, schema string, datum interface{}, buf []byte) {
	t.Helper()
	testBinaryDecodePass(t, schema, datum, buf)
	testBinaryEncodePass(t, schema, datum, buf)
}
