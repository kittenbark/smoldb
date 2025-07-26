package lexer

import (
	"io"

	"github.com/kittenbark/smoldb/ysmol/scanner"
	"github.com/kittenbark/smoldb/ysmol/token"
)

// Tokenize split to token instances from string
func Tokenize(src string) token.Tokens {
	var s scanner.Scanner
	s.Init(src)
	var tokens token.Tokens
	for {
		subTokens, err := s.Scan()
		if err == io.EOF {
			break
		}
		tokens.Add(subTokens...)
	}
	return tokens
}
