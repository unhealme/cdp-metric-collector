package main

import (
	"fmt"
	"io"
	"os"
	"runtime"

	arg "github.com/alexflint/go-arg"
)

type Arguments struct {
	Parser *arg.Parser    `arg:"-"`
	Outf   io.WriteCloser `arg:"-"`

	Paths    []string `arg:"positional,required" placeholder:"PATH"`
	Out      string   `arg:"-o,--" placeholder:"FILE" default:"-"`
	Append   bool     `arg:"--append" help:"append output to FILE"`
	DirOnly  bool     `arg:"--dir-only" help:"append output to FILE"`
	DateMax  DateTime `arg:"--older-than" placeholder:"TIME" default:"9999-12-31"`
	DateMin  DateTime `arg:"--newer-than" placeholder:"TIME"  default:"0000-01-01"`
	MaxDepth int      `arg:"-d,--max-depth" placeholder:"NUM"  default:"-1"`
	Verbose  bool     `arg:"-v,--verbose" help:"enable debug logging"`
}

func (a *Arguments) String() string {
	return fmt.Sprintf("%#v", a)
}

func (*Arguments) Version() string {
	return fmt.Sprintf("%s %s (%s-%s)", os.Args[0], Version, runtime.GOOS, runtime.GOARCH)
}

func ParseArgs() (*Arguments, error) {
	var (
		args   = &Arguments{}
		parser = arg.MustParse(args)
	)
	args.Parser = parser
	if args.Out == "-" {
		args.Outf = os.Stdout
	} else {
		flags := os.O_WRONLY | os.O_CREATE
		if args.Append {
			flags |= os.O_APPEND
		} else {
			flags |= os.O_TRUNC
		}
		f, err := os.OpenFile(args.Out, flags, 0644)
		if err != nil {
			return nil, err
		}
		args.Outf = f
	}
	return args, nil
}
