package lsmdb

import (
	"github.com/pkg/errors"
	"os"
)

//Close 表示去除冗余记录并关闭打开的数据库文件
func (db *LsmDB) Close() {
	db.Merge()
	db.dbFile.file.Close()
}

//PUT 表示向db中添加键值对key-value
func (db *LsmDB) PUT(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	entry := &Entry{key,value, uint32(len(key)), uint32(len(value)), PUT}
	enc, err := Encode(entry)
	if err != nil {
		return errors.Wrap(err, "将entry编码成字节切片过程中发生错误")
	}

	_, err = db.dbFile.file.WriteAt(enc, db.dbFile.offset)
	if err != nil {
		return errors.Wrap(err, "往数据库文件写入日志过程中发生错误")
	}
	db.indexs[string(key)] = db.dbFile.offset
	db.dbFile.offset += entry.GetSize()
	return nil
}

//GET 表示读取db中key对应的value
func (db *LsmDB) GET(key []byte) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	offset, ok := db.indexs[string(key)]
	if !ok {
		return nil,false, nil
	}
	entry, err := db.dbFile.Read(offset)
	if err != nil {
		return nil, false, errors.Wrap(err, "从数据库中读取记录过程中发生错误")
	}

	return entry.Value, true, nil
}

//DEL 表示删除db中存在的key-value
func (db *LsmDB) DEL(key, value []byte) error {
	db.mu.Lock()
	db.mu.Unlock()

	entry := &Entry{
		Key:       key,
		Value:     value,
		KeySize: uint32(len(key)),
		ValueSize: uint32(len(value)),
		Mark:      DEL,
	}

	enc, err := Encode(entry)
	if err != nil {
		return errors.Wrap(err, "将entry编码为切片数组过程中发生错误")
	}

	_, err = db.dbFile.file.WriteAt(enc, db.dbFile.offset)
	if err != nil {
		return errors.Wrap(err, "往数据库文件写入记录过程中发生错误")
	}
	delete(db.indexs, string(key))
	db.dbFile.offset += entry.GetSize()

	return nil
}

//Merge 表示去除数据库文件中冗余的记录
func (db *LsmDB) Merge() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	finalEntries, err := db.dbFile.Wash()
	if err != nil {
		return errors.Wrap(err, "去掉冗余记录过程中发生错误")
	}

	tmpFileName := db.dirPath + string(os.PathSeparator) + dbFileName + ".tmp"
	tmpFile, err := os.OpenFile(tmpFileName, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "调用库函数OpenFile来新建一个临时文件过程中发生错误")
	}
	defer tmpFile.Close()

	var offset int64
	for _, entry := range finalEntries {
		buf, err := Encode(entry)
		if err != nil {
			return errors.Wrap(err, "将清洗后的数据库文件中的记录转换为字节数组过程中发生错误")
		}
		tmpFile.WriteAt(buf, offset)
		db.indexs[string(entry.Key)] = offset
		offset += entry.GetSize()
	}
	db.dbFile.offset = offset

	stat, err := db.dbFile.file.Stat()
	if err != nil {
		return errors.Wrap(err, "调用库函数Stat获取数据库文件的状态信息过程中发生错误")
	}
	err = os.Remove(db.dirPath + string(os.PathSeparator) + stat.Name())
	if err != nil {
		return errors.Wrap(err, "调用库函数Remove删除数据库文件过程中发生错误")
	}
	fileName := db.dirPath + string(os.PathSeparator) + dbFileName
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return errors.Wrap(err, "调用库函数Rename将临时文件重名为为数据库文件名字过程中发生错误")
	}
	db.dbFile.file = tmpFile

	return nil
}