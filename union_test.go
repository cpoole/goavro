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
	"fmt"
	"math"
	"testing"
)

func TestSchemaUnion(t *testing.T) {
	testSchemaInvalid(t, `[{"type":"enum","name":"e1","symbols":["alpha","bravo"]},"e1"]`, "Union item 2 ought to be unique type")
	testSchemaInvalid(t, `[{"type":"enum","name":"com.example.one","symbols":["red","green","blue"]},{"type":"enum","name":"one","namespace":"com.example","symbols":["dog","cat"]}]`, "Union item 2 ought to be unique type")
}

type colors struct {
	val string
}

func (c colors) Str() string {
	return c.val
}

func (c colors) DeepCopy() interface{} {
	return &colors{c.val}
}

func TestUnion(t *testing.T) {
	testBinaryCodecPass(t, `["null","int"]`, nil, []byte("\x00"))

	// test null pointers
	var ptrInt *int
	testBinaryCodecPass(t, `["null","int"]`, ptrInt, []byte("\x00"))

	var three = 3
	ptrInt = &three
	testBinaryCodecPass(t, `["null","int"]`, ptrInt, []byte("\x02\x06"))

	testBinaryCodecPass(t, `["null","long"]`, ptrInt, []byte("\x02\x06"))

	colorEnum := &colors{"green"}
	testBinaryCodecPass(t, `["null", {"type":"enum","name":"colors","symbols":["red","green","blue"]}]`, colorEnum, []byte("\x02\x02"))

	colorEnum = &colors{"brown"}
	testBinaryEncodeFail(t, `["null", {"type":"enum","name":"colors","symbols":["red","green","blue"]}]`, colorEnum, "cannot encode binary enum \"colors\": value ought to be member of symbols: [red green blue]; \"brown\"")
}

func TestUnionRejectInvalidType(t *testing.T) {
	t.Helper()

	var maxUint uint64 = math.MaxUint64
	testBinaryEncodeFail(t, `["null","long"]`, &maxUint, "cannot encode binary long: uint would overflow")

	floatPtr := float64(3.5)
	testBinaryEncodeFail(t, `["null","int"]`, &floatPtr, "cannot encode binary int: provided Go float64 would lose precision: 3.500000")
}

func TestUnionWillCoerceTypeIfPossible(t *testing.T) {
	var int32val int32 = 3
	testBinaryCodecPass(t, `["null","long"]`, &int32val, []byte("\x02\x06"))
	var int64val int64 = 3
	testBinaryCodecPass(t, `["null","int"]`, &int64val, []byte("\x02\x06"))
	var float32val float32 = 3.5
	testBinaryCodecPass(t, `["null","double"]`, &float32val, []byte("\x02\x00\x00\x00\x00\x00\x00\f@"))
	var float64val float64 = 3.5
	testBinaryCodecPass(t, `["null","float"]`, &float64val, []byte("\x02\x00\x00\x60\x40"))
}

func TestUnionWithArray(t *testing.T) {
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, nil, []byte("\x00"))

	nilArray := []interface{}{}
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, &nilArray, []byte("\x02\x00"))

	oneArray := []interface{}{1}
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, &oneArray, []byte("\x02\x02\x02\x00"))

	twoArray := []interface{}{1, 2}
	testBinaryCodecPass(t, `["null",{"type":"array","items":"int"}]`, &twoArray, []byte("\x02\x04\x02\x04\x00"))
}

func TestUnionWithMap(t *testing.T) {
	testBinaryCodecPass(t, `["null",{"type":"map","values":"string"}]`, nil, []byte("\x00"))

	heMap := map[string]interface{}{"He": "Helium"}
	testBinaryCodecPass(t, `["null",{"type":"map","values":"string"}]`, &heMap, []byte("\x02\x02\x04He\x0cHelium\x00"))
}

func TestUnionMapRecordFitsInRecord(t *testing.T) {
	// union value may be either map or a record
	codec, err := NewCodec(`["null",{"type":"map","values":"double"},{"type":"record","name":"com.example.record","fields":[{"name":"field1","type":"int"},{"name":"field2","type":"float"}]}]`)
	if err != nil {
		t.Fatal(err)
	}

	// the provided datum value could be encoded by either the map or the record
	// schemas above
	datum := map[string]interface{}{
		"field1": 3,
		"field2": 3.5,
	}
	datumIn := datum

	buf, err := codec.BinaryFromNative(nil, datumIn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, []byte{
		0x04,                   // prefer record (union item 2) over map (union item 1)
		0x06,                   // field1 == 3
		0x00, 0x00, 0x60, 0x40, // field2 == 3.5
	}) {
		t.Errorf("GOT: %#v; WANT: %#v", buf, []byte{byte(2)})
	}

	// round trip
	datumOut, buf, err := codec.NativeFromBinary(buf)
	if err != nil {
		t.Fatal(err)
	}
	if actual, expected := len(buf), 0; actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}

	datumOutMap, ok := datumOut.(map[string]interface{})
	if !ok {
		t.Fatalf("GOT: %#v; WANT: %#v", ok, false)
	}
	if actual, expected := len(datumOutMap), 1; actual != expected {
		t.Fatalf("GOT: %#v; WANT: %#v", actual, expected)
	}
	datumValue, ok := datumOutMap["com.example.record"]
	if !ok {
		t.Fatalf("GOT: %#v; WANT: %#v", datumOutMap, "have `com.example.record` key")
	}
	datumValueMap, ok := datumValue.(map[string]interface{})
	if !ok {
		t.Errorf("GOT: %#v; WANT: %#v", ok, true)
	}
	if actual, expected := len(datumValueMap), len(datum); actual != expected {
		t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
	}
	for k, v := range datum {
		if actual, expected := fmt.Sprintf("%v", datumValueMap[k]), fmt.Sprintf("%v", v); actual != expected {
			t.Errorf("GOT: %#v; WANT: %#v", actual, expected)
		}
	}
}

func TestUnionRecordFieldWhenNull(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "r1",
  "fields": [
    {"name": "f1", "type": ["null", {"type": "array", "items": "string"}]}
  ]
}`
	unknownBullshitType := []interface{}{}
	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": &unknownBullshitType}, []byte("\x02\x00"))

	strArray := []string{"bar"}
	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": &strArray}, []byte("\x02\x02\x06bar\x00"))

	emptyStrArray := []string{}
	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": &emptyStrArray}, []byte("\x02\x00"))
	testBinaryCodecPass(t, schema, map[string]interface{}{"f1": nil}, []byte("\x00"))
}

func TestUnionText(t *testing.T) {
	testTextCodecPass(t, `["null","int"]`, nil, []byte("null"))
	val := 3
	testTextCodecPass(t, `["null","int"]`, &val, []byte(`{"int":3}`))
	strVal := "😂 "
	testTextCodecPass(t, `["null","string"]`, &strVal, []byte(`{"string":"\u0001\uD83D\uDE02 "}`))
}

func ExampleJSONUnion() {
	codec, err := NewCodec(`["null","string"]`)
	if err != nil {
		fmt.Println(err)
	}
	val := "some string"
	buf, err := codec.TextualFromNative(nil, &val)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(buf))
	// Output: {"string":"some string"}
}

//
// The following examples show the way to put a new codec into use
// Currently the only new codec is ont that supports standard json
// which does not indicate unions in any way
// so standard json data needs to be guided into avro unions

// show how to use the default codec via the NewCodecFrom mechanism
func ExampleCustomCodec() {
	codec, err := NewCodecFrom(`"string"`, &codecBuilder{
		buildCodecForTypeDescribedByMap,
		buildCodecForTypeDescribedByString,
		buildCodecForTypeDescribedBySlice,
	})
	if err != nil {
		fmt.Println(err)
	}
	buf, err := codec.TextualFromNative(nil, "some string 22")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(buf))
	// Output: "some string 22"
}

// Use the standard JSON codec instead
func ExampleJSONStringToTextual() {
	codec, err := NewCodecFrom(`["null","string"]`, &codecBuilder{
		buildCodecForTypeDescribedByMap,
		buildCodecForTypeDescribedByString,
		buildCodecForTypeDescribedBySliceJSON,
	})
	if err != nil {
		fmt.Println(err)
	}

	val := "some string"
	buf, err := codec.TextualFromNative(nil, &val)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(buf))
	// Output: {"string":"some string"}
}

func ExampleJSONStringToNative() {
	codec, err := NewCodecFrom(`["null","string"]`, &codecBuilder{
		buildCodecForTypeDescribedByMap,
		buildCodecForTypeDescribedByString,
		buildCodecForTypeDescribedBySliceJSON,
	})
	if err != nil {
		fmt.Println(err)
	}
	// send in a legit json string
	t, _, err := codec.NativeFromTextual([]byte("\"some string one\""))
	if err != nil {
		fmt.Println(err)
	}
	// see it parse into a map like the avro encoder does
	o, ok := t.(map[string]interface{})
	if !ok {
		fmt.Printf("its a %T not a map[string]interface{}", t)
	}

	// pull out the string to show its all good
	_v := o["string"]
	v, ok := _v.(string)
	fmt.Println(v)
	// Output: some string one
}

func TestUnionJSON(t *testing.T) {
	testJSONDecodePass(t, `["null","int"]`, nil, []byte("null"))
	int3 := 3
	testJSONDecodePass(t, `["null","int"]`, &int3, []byte(`3`))
	long33 := 333333333333333
	testJSONDecodePass(t, `["null","long"]`, &long33, []byte(`333333333333333`))
	float6 := 6.77
	testJSONDecodePass(t, `["null","float"]`, &float6, []byte(`6.77`))
	//double6 := 6.77
	//testJSONDecodePass(t, `["null","double"]`, &double6, []byte(`6.77`))
	//testJSONDecodePass(t, `["null",{"type":"array","items":"int"}]`, Union("array", []interface{}{1, 2}), []byte(`[1,2]`))
	//testJSONDecodePass(t, `["null",{"type":"map","values":"int"}]`, Union("map", map[string]interface{}{"k1": 13}), []byte(`{"k1":13}`))
	//testJSONDecodePass(t, `["null",{"name":"r1","type":"record","fields":[{"name":"field1","type":"string"},{"name":"field2","type":"string"}]}]`, Union("r1", map[string]interface{}{"field1": "value1", "field2": "value2"}), []byte(`{"field1": "value1", "field2": "value2"}`))
	//testJSONDecodePass(t, `["null","boolean"]`, Union("boolean", true), []byte(`true`))
	//testJSONDecodePass(t, `["null","boolean"]`, Union("boolean", false), []byte(`false`))
	//testJSONDecodePass(t, `["null",{"type":"enum","name":"e1","symbols":["alpha","bravo"]}]`, Union("e1", "bravo"), []byte(`"bravo"`))
	//testJSONDecodePass(t, `["null", "bytes"]`, Union("bytes", []byte("")), []byte("\"\""))
	//testJSONDecodePass(t, `["null", "bytes", "string"]`, Union("bytes", []byte("")), []byte("\"\""))
	//testJSONDecodePass(t, `["null", "string", "bytes"]`, Union("string", "value1"), []byte(`"value1"`))
	//testJSONDecodePass(t, `["null", {"type":"enum","name":"e1","symbols":["alpha","bravo"]}, "string"]`, Union("e1", "bravo"), []byte(`"bravo"`))
	//testJSONDecodePass(t, `["null", {"type":"fixed","name":"f1","size":4}]`, Union("f1", []byte(`abcd`)), []byte(`"abcd"`))
	//testJSONDecodePass(t, `"string"`, "abcd", []byte(`"abcd"`))
	//testJSONDecodePass(t, `{"type":"record","name":"kubeEvents","fields":[{"name":"field1","type":"string","default":""}]}`, map[string]interface{}{"field1": "value1"}, []byte(`{"field1":"value1"}`))
	//testJSONDecodePass(t, `{"type":"record","name":"kubeEvents","fields":[{"name":"field1","type":"string","default":""},{"name":"field2","type":"string"}]}`, map[string]interface{}{"field1": "", "field2": "deef"}, []byte(`{"field2": "deef"}`))
	//testJSONDecodePass(t, `{"type":"record","name":"kubeEvents","fields":[{"name":"field1","type":["string","null"],"default":""}]}`, map[string]interface{}{"field1": Union("string", "value1")}, []byte(`{"field1":"value1"}`))
	//testJSONDecodePass(t, `{"type":"record","name":"kubeEvents","fields":[{"name":"field1","type":["string","null"],"default":""}]}`, map[string]interface{}{"field1": nil}, []byte(`{"field1":null}`))
	//// union of null which has minimal syntax
	//testJSONDecodePass(t, `{"type":"record","name":"LongList","fields":[{"name":"next","type":["null","LongList"],"default":null}]}`, map[string]interface{}{"next": nil}, []byte(`{"next": null}`))
	//// record containing union of record (recursive record)
	//testJSONDecodePass(t, `{"type":"record","name":"LongList","fields":[{"name":"next","type":["null","LongList"],"default":null}]}`, map[string]interface{}{"next": Union("LongList", map[string]interface{}{"next": nil})}, []byte(`{"next":{"next":null}}`))
	//testJSONDecodePass(t, `{"type":"record","name":"LongList","fields":[{"name":"next","type":["null","LongList"],"default":null}]}`, map[string]interface{}{"next": Union("LongList", map[string]interface{}{"next": Union("LongList", map[string]interface{}{"next": nil})})}, []byte(`{"next":{"next":{"next":null}}}`))
}
