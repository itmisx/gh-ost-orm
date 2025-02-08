package gh_ost_orm

import (
	"bufio"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
)

type orm struct {
	binPath  string
	host     string
	user     string
	password string
	database string
	table    string
	flags    map[string]string
	alter    string
}

type FlagOption func(*orm)

func WithFlag(pk string, pv ...string) FlagOption {
	return func(o *orm) {
		if len(pv) > 0 {
			o.flags[pk] = pv[0]
		} else {
			o.flags[pk] = ""
		}
	}
}

// AssumeMasterHost
// gh-ost infers the identity of the master server by crawling up the replication topology.
// You may explicitly tell gh-ost the identity of the master host via --assume-master-host=the.master.com.
// This is useful in:
// - master-master topologies (together with --allow-master-master), where gh-ost can arbitrarily pick one of the co-masters, and you prefer that it picks a specific one
// - tungsten replicator topologies (together with --tungsten), where gh-ost is unable to crawl and detect the master
func WithAssumeMasterHostFlag(host string) FlagOption {
	return func(o *orm) {
		o.flags["assume-master-host"] = host
	}
}

// New orm配置
// binPath gh-ost文件路径
func New(binPath, host, user, password, database, table string, flagOptions ...FlagOption) *orm {
	o := &orm{}
	o.binPath = binPath
	o.host = host
	o.user = user
	o.password = password
	o.database = database
	o.table = table
	o.flags = make(map[string]string)
	for _, option := range flagOptions {
		option(o)
	}
	return o
}

// SetEngine 设置存储引擎类型
func (o *orm) SetEngine(engine string) *orm {
	o.alter = "engine=" + engine
	return o
}

// AddColumn 添加列
// charset: set ... collate ...
func (o *orm) AddColumn(columnName string, _type string, length, decimal int, charset string, notNULL bool, _default string, comment string) *orm {
	var statement []string
	statement = append(statement, "ADD COLUMN")
	if columnName != "" {
		statement = append(statement, columnName)
	}
	if _type != "" {
		columnType := _type
		if length > 0 && decimal > 0 {
			columnType += "(" + strconv.Itoa(length) + "," + strconv.Itoa(decimal) + ")"
		} else if length > 0 {
			columnType += "(" + strconv.Itoa(length) + ")"
		}
		statement = append(statement, columnType)
	}

	if charset != "" {
		statement = append(statement, "CHARACTER "+charset)
	}

	if notNULL {
		statement = append(statement, "NOT NULL")
	}

	if _default != "" {
		statement = append(statement, "DEFAULT "+_default)
	}

	if comment != "" {
		statement = append(statement, "COMMENT '"+comment+"'")
	}
	o.alter = strings.Join(statement, " ")
	return o
}

// ModifyColumn 修改列
func (o *orm) ModifyColumn(columnName string, _type string, length, decimal int, charset string, notNULL bool, _default, comment string) *orm {
	var statement []string
	statement = append(statement, "MODIFY COLUMN")
	if columnName != "" {
		statement = append(statement, columnName)
	}
	if _type != "" {
		columnType := _type
		if length > 0 && decimal > 0 {
			columnType += "(" + strconv.Itoa(length) + "," + strconv.Itoa(decimal) + ")"
		} else if length > 0 {
			columnType += "(" + strconv.Itoa(length) + ")"
		}
		statement = append(statement, columnType)
	}

	if charset != "" {
		statement = append(statement, "CHARACTER "+charset)
	}

	if notNULL {
		statement = append(statement, "NOT NULL")
	}

	if _default != "" {
		statement = append(statement, "DEFAULT "+_default)
	}

	if comment != "" {
		statement = append(statement, "COMMENT '"+comment+"'")
	}
	o.alter = strings.Join(statement, " ")
	return o
}

// RemoveColumn 删除列
func (o *orm) RemoveColumn(columnName string) {
	o.alter = "DROP COLUMN " + columnName
}

// AddIndex 添加索引
func (o *orm) AddIndex(indexName string, indexType string, indexMethod string, columns ...string) *orm {
	statement := ""
	if indexType != "" {
		statement = "ADD " + string(indexType)
	} else {
		statement = "ADD"
	}
	statement += " INDEX " + indexName
	if len(columns) == 0 {
		panic("add index panic: columns can not be empty")
	}

	statement += "(" + strings.Join(columns, ",") + ")"

	if indexMethod != "" {
		statement += " USING " + indexMethod
	}
	o.alter = statement
	return o
}

// RemoveIndex 删除索引
func (o *orm) RemoveIndex(indexName string) *orm {
	o.alter = "DROP INDEX " + indexName
	return o
}

// Execute 执行迁移
func (o *orm) Execute() error {
	if o.alter == "" {
		panic("alter statement can not be empty")
	}
	args := []string{}
	args = append(args, "--host="+o.host)
	args = append(args, "--user="+o.user)
	args = append(args, "--password="+o.password)
	args = append(args, "--database="+o.database)
	args = append(args, "--table="+o.table)
	args = append(args, "--alter="+o.alter)
	args = append(args, "--allow-on-master")
	// MySQL 8.0 supports "instant DDL" for some operations. If an alter statement can be completed with instant DDL, only a metadata change is required internally. Instant operations include:
	// Adding a column
	// Dropping a column
	// Dropping an index
	// Extending a varchar column
	// Adding a virtual generated column
	args = append(args, "--attempt-instant-ddl")
	args = append(args, "--initially-drop-ghost-table")
	args = append(args, "--initially-drop-old-table")
	// 自定义参数
	for flagKey, flagVal := range o.flags {
		if flagVal == "" {
			args = append(args, "--"+flagKey)
		} else {
			args = append(args, "--"+flagKey+"="+flagVal)
		}
	}
	args = append(args, "--execute")

	cmd := exec.Command(o.binPath, args...)
	log.Println("原始迁移语句", cmd.String())

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Fatal(err)
	}

	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
	}()

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}
	}()

	// 等待命令完成
	if err := cmd.Wait(); err != nil {
		return err
	}
	return nil
}
