//+build !linux

package main

import "syscall"

func procAttr() *syscall.SysProcAttr { return nil }
