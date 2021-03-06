package drbuffer

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

type Assert func(left interface{}, op string, right interface{})

func NewAssert(t *testing.T) Assert {
	return func(left interface{}, op string, right interface{}) {
		switch op {
		case "==":
			assertEq(t, left, right)
		case "!=":
			assertNe(t, left, right)
		case ">":
			assertGt(t, left, right)
		default:
			t.Fatal("unsupported operator", op)
		}
	}
}

func assertNe(t *testing.T, left interface{}, right interface{}) {
	if checkEq(t, left, right) {
		Fail(t, "%s -= %s", left, right)
	}
}

func assertEq(t *testing.T, left interface{}, right interface{}) {
	if !checkEq(t, left, right) {
		Fail(t, "%s != %s", left, right)
	}
}
func checkEq(t *testing.T, left interface{}, right interface{}) bool {
	if right == nil {
		if left == nil {
			return true
		}
		leftVal := reflect.ValueOf(left)
		switch leftVal.Kind() {
		case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Interface, reflect.Slice:
			return leftVal.IsNil()
		default:
			return false
		}
	}
	return reflect.DeepEqual(left, right)
}

func assertGt(t *testing.T, left interface{}, right interface{}) {
	if !checkGt(t, left, right) {
		Fail(t, "%s <= %s", left, right)
	}
}

func checkGt(t *testing.T, left interface{}, right interface{}) bool {
	leftAsInt, ok := left.(int)
	if ok {
		rightAsInt, ok := right.(int)
		if ok {
			return leftAsInt > rightAsInt
		} else {
			Fail(t, "%s not comparable to %s", reflect.TypeOf(left), reflect.TypeOf(right))
		}
	}
	leftAsUint32, ok := left.(uint32)
	if ok {
		rightAsUint32, ok := right.(uint32)
		if ok {
			return leftAsUint32 > rightAsUint32
		} else {
			Fail(t, "%s not comparable to %s", reflect.TypeOf(left), reflect.TypeOf(right))
		}
	}
	Fail(t, "%s not comparable to %s", reflect.TypeOf(left), reflect.TypeOf(right))
	return false
}

// Fail reports a failure through
func Fail(t *testing.T, failureMessage string, a ...interface{}) {
	errorTrace := strings.Join(CallerInfo(), "\n\r\t\t\t")
	t.Fatalf("\r%s\r\tError Trace:\t%s\n"+
		"\r\tError:%s\n\r",
		getWhitespaceString(),
		errorTrace,
		indentMessageLines(fmt.Sprintf(failureMessage, a...), 2))
}

/* CallerInfo is necessary because the assert functions use the testing object
internally, causing it to print the file:line of the assert method, rather than where
the problem actually occured in calling code.*/

// CallerInfo returns an array of strings containing the file and line number
// of each stack frame leading from the current test to the assert call that
// failed.
func CallerInfo() []string {

	pc := uintptr(0)
	file := ""
	line := 0
	ok := false
	name := ""

	callers := []string{}
	for i := 0; ; i++ {
		pc, file, line, ok = runtime.Caller(i)
		if !ok {
			return nil
		}

		// This is a huge edge case, but it will panic if this is the case, see #180
		if file == "<autogenerated>" {
			break
		}

		parts := strings.Split(file, "/")
		dir := parts[len(parts)-2]
		file = parts[len(parts)-1]
		if (dir != "assert" && dir != "mock" && dir != "require") || file == "mock_test.go" {
			callers = append(callers, fmt.Sprintf("%s:%d", file, line))
		}

		f := runtime.FuncForPC(pc)
		if f == nil {
			break
		}
		name = f.Name()
		// Drop the package
		segments := strings.Split(name, ".")
		name = segments[len(segments)-1]
		if isTest(name, "Test") ||
			isTest(name, "Benchmark") ||
			isTest(name, "Example") {
			break
		}
	}

	return callers
}

// Stolen from the `go test` tool.
// isTest tells whether name looks like a test (or benchmark, according to prefix).
// It is a Test (say) if there is a character after Test that is not a lower-case letter.
// We don't want TesticularCancer.
func isTest(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	if len(name) == len(prefix) {
		// "Test" is ok
		return true
	}
	rune, _ := utf8.DecodeRuneInString(name[len(prefix):])
	return !unicode.IsLower(rune)
}

// getWhitespaceString returns a string that is long enough to overwrite the default
// output from the go testing framework.
func getWhitespaceString() string {

	_, file, line, ok := runtime.Caller(1)
	if !ok {
		return ""
	}
	parts := strings.Split(file, "/")
	file = parts[len(parts)-1]

	return strings.Repeat(" ", len(fmt.Sprintf("%s:%d:      ", file, line)))

}

// Indents all lines of the message by appending a number of tabs to each line, in an output format compatible with Go's
// test printing (see inner comment for specifics)
func indentMessageLines(message string, tabs int) string {
	outBuf := new(bytes.Buffer)

	for i, scanner := 0, bufio.NewScanner(strings.NewReader(message)); scanner.Scan(); i++ {
		if i != 0 {
			outBuf.WriteRune('\n')
		}
		for ii := 0; ii < tabs; ii++ {
			outBuf.WriteRune('\t')
			// Bizarrely, all lines except the first need one fewer tabs prepended, so deliberately advance the counter
			// by 1 prematurely.
			if ii == 0 && i > 0 {
				ii++
			}
		}
		outBuf.WriteString(scanner.Text())
	}

	return outBuf.String()
}
