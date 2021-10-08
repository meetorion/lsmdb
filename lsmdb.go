package lsmdb

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"os"
	"sync"
)

//LsmDB 表示数据库需要哪些字段来完成功能
type LsmDB struct {
	indexs map[string]int64
	dirPath string
	dbFile *dbFile
	mu sync.Mutex
}

func init() {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
}

const dbFileName = "lsm.data"

//Open 表示根据dirPath指定的目录创建一个数据库文件名为dbFileName的数据库
func Open(dirPath string) (*LsmDB, error) {
	err := CreatNx(dirPath)
	if err != nil {
		return nil, errors.Wrap(err, "创建目录错误")
	}

	//在目录dirPath下读取/创建一个文件
	filename := dirPath + string(os.PathSeparator) + dbFileName
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "在目录dirPath下新建/打开数据库文件过程中发生错误")
	}

	stat, err := file.Stat()
	if err != nil {
		//zap.S().Error(err)
		return nil, errors.Wrap(err, "获取文件状态错误")
	}
	dbFile := &dbFile{file, stat.Size()}

	index, err := loadIndexFromFile(dbFile)
	if err != nil {
		return nil, errors.Wrap(err, "加载索引错误")
	}

	db := &LsmDB{index,dirPath,dbFile, sync.Mutex{}}
	return db, nil
}

func loadIndexFromFile(dbFile *dbFile) (map[string]int64, error) {
	stat, err := dbFile.file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "获取文件状态错误")
	}
	if stat.Size() == 0 {
		return make(map[string]int64, 0), nil
	}

	var (
		offset int64
	)
	indexs := make(map[string]int64)

	for {
		entry, err := dbFile.Read(offset)
		if err != nil {
			if err != io.EOF {
				return nil, errors.Wrap(err, "从数据库文件中读取日志发生错误")
			}
			break

		}

		if entry.Mark == DEL {
			delete(indexs, string(entry.Key))
		} else {
			indexs[string(entry.Key)] = offset
		}
		offset += entry.GetSize()
	}

	return indexs, nil
}
