package cassCrudManager

import (
	"fmt"

	"cassandra_gocql_crud_operations/model"

	"github.com/gocql/gocql"
)

func InsertData(emp model.Profile, tableName string, Session *gocql.Session) error {
	fmt.Println(" **** Inserting new data ****\n", emp.Key)
	if err := Session.Query(fmt.Sprintf("INSERT INTO example_database.%s(key, value, tel, age, name) VALUES(?, ?, ?, ?, ?)", tableName),
		emp.Key, emp.Value, emp.Tel, emp.Age, emp.Name).Exec(); err != nil {
		fmt.Println("InsertData Error : ", err.Error())
		return err
	}
	fmt.Println("Successfully Added")
	return nil
}

func GetByKey(tableName string, key string, value int16, Session *gocql.Session) (*model.Profile, error) {
	data := model.Profile{}
	if err := Session.Query(fmt.Sprintf("SELECT key, value, tel, age, name FROM example_database.%s WHERE key = ? AND value=? LIMIT 1 ", tableName),
		key, value).Scan(&data.Key, &data.Value, &data.Tel, &data.Age, &data.Name); err != nil {
		fmt.Println("GetByKey Error : ", err.Error())
		return nil, err
	}
	return &data, nil
}

func GetAllData(tableName string, Session *gocql.Session) (*[]model.Profile, error) {
	var data model.Profile
	var models []model.Profile
	iter := Session.Query(fmt.Sprintf(`SELECT key, value, tel, age, name FROM example_database.%s`, tableName)).Iter()
	for iter.Scan(&data.Key, &data.Value, &data.Tel, &data.Age, &data.Name) {
		models = append(models, data)
	}
	if err := iter.Close(); err != nil {
		fmt.Println("GetAllData Error : ", err.Error())
		return nil, err
	}
	return &models, nil
}

func GetMaxByValue(tableName string, Session *gocql.Session) (int16, error) {

	data := &model.Profile{}
	if err := Session.Query(fmt.Sprintf("SELECT MAX(value) FROM example_database.%s LIMIT 1", tableName)).Scan(&data.Value); err != nil {
		return 0, err
	}
	return int16(data.Value), nil

}

func UpdateById(tableName string, key string, value int16, data model.Profile, Session *gocql.Session) error {
	if err := Session.Query(fmt.Sprintf("UPDATE example_database.%s SET tel=?, age=?, name=? WHERE key = ? AND value = ?", tableName),
		data.Tel, data.Age, data.Name, key, value).Exec(); err != nil {
		return err
	}

	return nil
}

func DeleteById(tableName string, key string, value int16, Session *gocql.Session) error {
	if err := Session.Query(fmt.Sprintf("DELETE FROM example_database.%s WHERE key = ? AND value = ?", tableName),
		key, value).Exec(); err != nil {
		fmt.Println("Delete Error : ", err.Error())
		return err
	}
	fmt.Println("Deleted Data")
	return nil
}
