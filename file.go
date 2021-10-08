package lsmdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/pkg/errors"
)

type dbFile struct {
	file   *os.File
	offset int64
}

const (
	//PUT 表示记录类型是新增
	PUT uint16 = iota
	//DEL 表示记录类型是删除
	DEL
)

const entryHeaderSize = 10

//Entry 表示向数据库文件写入或读取的最小单位——记录
type Entry struct {
	Key       []byte
	Value     []byte
	KeySize   uint32
	ValueSize uint32
	Mark      uint16
}

//GetSize 表示获取e的字节长度
func (e *Entry) GetSize() int64 {
	return int64(entryHeaderSize + e.KeySize + e.ValueSize)
}

//Read 表示从d中的offset位置开始读取一条记录
func (d *dbFile) Read(offset int64) (Entry, error) {
	buf := make([]byte, entryHeaderSize)
	if _, err := d.file.ReadAt(buf, offset); err == io.EOF {
		return Entry{}, err
	} else if err != nil {
		return Entry{}, errors.Wrap(err, "调用ReadAt方法读取头信息过程中发生错误")
	}

	var (
		entry Entry
		err   error
	)
	if entry, err = Decode(buf); err != nil {
		return entry, errors.Wrap(err, "解码Entry头信息过程中发生错误")
	}

	Key := make([]byte, entry.KeySize)
	Value := make([]byte, entry.ValueSize)
	offset += int64(entryHeaderSize)
	if _, err := d.file.ReadAt(Key, offset); err != nil {
		return entry, errors.Wrap(err, "调用ReadAt方法读取Key过程中发生错误")
	}
	entry.Key = Key

	offset += int64(entry.KeySize)
	if _, err := d.file.ReadAt(Value, offset); err != nil {
		return entry, errors.Wrap(err, "调用ReadAt方法读取Value过程中发生错误")
	}
	entry.Value = Value

	return entry, nil
}

func (d *dbFile) Wash() (map[string]*Entry, error) {
	var offset int64
	ret := make(map[string]*Entry)
	for {
		entry, err := d.Read(offset)

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, errors.Wrap(err, "从数据库文件中读取记录过程发生错误")
		}

		if entry.Mark == DEL {
			delete(ret, string(entry.Key))
		} else {
			ret[string(entry.Key)] = &entry
		}
		offset += entry.GetSize()
	}
	return ret, nil
}

//Decode 表示将buf转换为Entry格式
func Decode(buf []byte) (Entry, error) {
	ret := Entry{}
	err := binary.Read(bytes.NewBuffer(buf[0:4]), binary.BigEndian, &ret.KeySize)
	if err != nil {
		return ret, errors.Wrap(err, "解码KeySize值过程中发生错误")
	}
	err = binary.Read(bytes.NewBuffer(buf[4:8]), binary.BigEndian, &ret.ValueSize)
	if err != nil {
		return ret, errors.Wrap(err, "解码ValueSize值过程中发生错误")
	}
	err = binary.Read(bytes.NewBuffer(buf[8:10]), binary.BigEndian, &ret.Mark)
	if err != nil {
		return ret, errors.Wrap(err, "解码Mark值过程中发生错误")
	}

	return ret, nil
}

//Encode 表示将entry转换为字节数组形式
func Encode(entry *Entry) ([]byte, error) {
	buf := make([]byte, entry.GetSize())
	err := binary.Write(bytes.NewBuffer(buf[0:3]), binary.LittleEndian, entry.KeySize)
	if err != nil {
		return buf, errors.Wrap(err, "往字节数组中写KeySize过程中发生错误")
	}

	err = binary.Write(bytes.NewBuffer(buf[4:7]), binary.LittleEndian, entry.ValueSize)
	if err != nil {
		return buf, errors.Wrap(err, "往字节数组中写ValueSize过程中发生错误")
	}

	err = binary.Write(bytes.NewBuffer(buf[8:9]), binary.LittleEndian, entry.Mark)
	if err != nil {
		return buf, errors.Wrap(err, "往字节数组中写Mark过程中发生错误")
	}

	copy(buf[entryHeaderSize:entryHeaderSize+entry.KeySize], entry.Key)
	copy(buf[entryHeaderSize+entry.KeySize:entryHeaderSize+entry.KeySize+entry.ValueSize], entry.Value)
	return buf, nil
}

//CreatNx 表示如果目录dirPath不存在则创建
func CreatNx(dirPath string) error {
	if !exists(dirPath) {
		err := os.MkdirAll(dirPath, os.ModePerm)
		if err != nil {
			return errors.Wrap(err, "创建目录失败")
		}
	}
	return nil
}

func exists(dirPath string) bool {
	_, err := os.Stat(dirPath)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
	}
	return false
}
