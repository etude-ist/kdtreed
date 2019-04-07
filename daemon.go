package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/kyroy/kdtree"
	"github.com/kyroy/kdtree/points"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
)

type Data struct {
	value int
}

type ServerConfig struct {
	Host string
	Port string
}

type Expr struct {
	buffer   string
	position int
	action   string
	point    []float64
	data     Data
	valid    bool
}

func (expr *Expr) Current() string {
	return expr.buffer[expr.position:]
}

func (expr *Expr) SkipWhitespace() {
	for x := range expr.buffer[expr.position:] {
		if string(x) != " " {
			break
		}
		expr.position += 1
	}
}

func ReadConfig(fname *string) ServerConfig {
	var config ServerConfig
	if _, err := toml.DecodeFile(*fname, &config); err != nil {
		log.Fatal(err)
	}
	return config
}

func Match(expr *Expr, token string) (string, bool) {
	expr.SkipWhitespace()
	re, err := regexp.Compile(token)
	if err != nil {
		return "", false
	}
	m := re.Find([]byte(expr.Current()))
	if m != nil {
		expr.position += len(m)
		return string(m), true
	}
	return string(m), false
}

func IsAction(expr *Expr) bool {
	if token, status := Match(expr, "ADD|DEL|KNN|END"); status {
		expr.action = token
		return true
	}
	expr.position = 0
	return false
}

func IsEndAction(expr *Expr) bool {
	rst := IsAction(expr)
	if expr.action == "END" {
		return rst
	}
	expr.position = 0
	return false
}

func IsPoint(expr *Expr) bool {
	if token, status := Match(expr, "{[0-9]+, [0-9]+}"); status {
		expr.point = MakePoint(token)
		return true
	}
	expr.position = 0
	return false
}

func IsData(expr *Expr) bool {
	if token, status := Match(expr, "[0-9]+"); status {
		value, _ := strconv.Atoi(token)
		expr.data = Data{value: value}
		return true
	}
	expr.position = 0
	return false
}

func IsCommand(expr *Expr) bool {
	return IsAction(expr) && IsPoint(expr) && IsData(expr)
}

func IsAddCommand(expr *Expr) bool {
	rst := IsCommand(expr)
	if expr.action == "ADD" {
		return rst
	}
	expr.position = 0
	return false
}

func IsKnnCommand(expr *Expr) bool {
	rst := IsCommand(expr)
	if expr.action == "KNN" {
		return rst
	}
	expr.position = 0
	return false
}

func IsPartialCommand(expr *Expr) bool {
	return IsAction(expr) && IsPoint(expr)
}

func IsDelCommand(expr *Expr) bool {
	rst := IsPartialCommand(expr)
	if expr.action == "DEL" {
		return rst
	}
	expr.position = 0
	return false
}

func IsFullCommand(expr *Expr) bool {
	return IsAddCommand(expr) || IsKnnCommand(expr)
}

func ParseKDtreeCommand(command string) Expr {
	command = strings.TrimSpace(command)
	var expr Expr
	expr.buffer = command
	expr.valid = false
	valid := IsFullCommand(&expr) || IsDelCommand(&expr) || IsEndAction(&expr)
	if valid {
		expr.valid = true
	}
	return expr
}

func MakePoint(p string) []float64 {
	re := regexp.MustCompile("[0-9]+")
	rst := re.FindAllString(p, -1)
	x, _ := strconv.Atoi(rst[0])
	y, _ := strconv.Atoi(rst[1])
	return []float64{float64(x), float64(y)}
}

func HandleRequest(connection net.Conn, tree *kdtree.KDTree) {
	connection.Write([]byte("Connected to kdtreed...\r\n"))
	for {
		data, err := bufio.NewReader(connection).ReadString('\n')
		if err != nil {
			log.Fatal(err)
			return
		}

		parsed := ParseKDtreeCommand(data)
		if !parsed.valid {
			break
		}
		if parsed.valid && parsed.action == "END" {
			break
		}

		switch parsed.action {
		case "ADD":
			tree.Insert(points.NewPoint(parsed.point, parsed.data))
			connection.Write([]byte(fmt.Sprintf("%+v added\r\n", parsed.point)))
		case "DEL":
			tree.Remove(&points.Point{Coordinates: parsed.point})
			connection.Write([]byte(fmt.Sprintf("%+v deleted\r\n", parsed.point)))
		case "KNN":
			rst := tree.KNN(&points.Point{Coordinates: parsed.point}, parsed.data.value)
			connection.Write([]byte(fmt.Sprintf("%+v\r\n", rst)))
		}

	}
	connection.Close()
}

func main() {
	fname := flag.String("config", "config.toml", "-config=<file_name>")
	flag.Parse()
	config := ReadConfig(fname)

	listener, err := net.Listen("tcp4", config.Host+":"+config.Port)
	if err != nil {
		log.Fatal(err)
	}

	defer listener.Close()
	fmt.Println("Started kdtreed on HOST:", config.Host, "PORT:", config.Port)

	tree := kdtree.New([]kdtree.Point{})

	for {
		request, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
			return
		}
		go HandleRequest(request, tree)
	}
}
