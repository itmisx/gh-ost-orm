package gh_ost_orm

import (
	"fmt"
	"testing"
)

func TestORMSQL(t *testing.T) {
	orm := New("./gh-ost", "127.0.0.1", "root", "123456", "test", "test")
	orm.AddColumn("c1", "varchar", 10, 0, "", true, "'a'", "test")
	fmt.Println(orm.alter)
	orm.ModifyColumn("c1", "decimal", 10, 3, "", true, "0.00", "test")
	fmt.Println(orm.alter)
	orm.RemoveColumn("c1")
	fmt.Println(orm.alter)
	orm.AddIndex("idx_c1", "UNIQUE", "BTREE", "c1")
	fmt.Println(orm.alter)
	orm.RemoveIndex("idx_c1")
	fmt.Println(orm.alter)
}

func TestAddColumn(t *testing.T) {
	{
		orm := New("./gh-ost", "127.0.0.1", "root", "123456", "test", "test")
		err := orm.ModifyColumn("c7", "varchar", 300, 0, "", true, "'1'", "c2").Execute()
		fmt.Println(err)
	}
}
