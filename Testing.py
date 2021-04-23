import unittest
from moto import mock_dynamodb2
from Ques_3.Functions import CrudOperations


class TestTask(unittest.TestCase):
    @mock_dynamodb2
    def test_insertData(self):
        obj1 = CrudOperations()
        obj1.create_table_('test1')
        res = obj1.insert_item(506, 10, 'Tapish', 'Roy', 'D', 'test1')
        try:
            self.assertEqual(200, res['ResponseMetadata']['HTTPStatusCode'])
            print("Test For Insert Data Success")
        except:
            print('Test Case for method insert_item() Failed')

    @mock_dynamodb2
    def test_getData(self):
        obj2 = CrudOperations()
        obj2.create_table_('test1')
        obj2.insert_item(506, 10, 'Tapish', 'Roy', 'D', 'test1')
        result = obj2.get_items(506, 'test1')
        expected = {'Last_name': 'Roy', 'section': 'D', 'First_name': 'Tapish', 'RollNo': 506, 'Age': 10}
        try:
            self.assertEqual(result, expected)
            print('Test For get Item Success')
        except:
            print('Test Case for Method get_items Failed')

    @mock_dynamodb2
    def test_updateData(self):
        obj3 = CrudOperations()
        obj3.create_table_('test1')
        obj3.insert_item(506, 10, 'Tapish', 'Roy', 'D', 'test1')
        obj3.update_item_(506, 14, 'test1')
        updated_result = obj3.get_items(506, 'test1')
        expected = {'Last_name': 'Roy', 'section': 'D', 'First_name': 'Tapish', 'RollNo': 506, 'Age': 14}
        try:
            self.assertEqual(updated_result, expected)
            print("Test For Update Data Success")
        except:
            print('Test Case for Method update_item() Failed')

    @mock_dynamodb2
    def test_deleteData(self):
        obj4 = CrudOperations()
        obj4.create_table_('test1')
        obj4.insert_item(506, 10, 'Tapish', 'Roy', 'D', 'test1')
        delete = obj4.delete_item_(506, 'test1')
        try:
            self.assertEqual(200, delete['ResponseMetadata']['HTTPStatusCode'])
            print("Test For Delete Data Success")
        except:
            print('Test Case for Method delete_item() Failed')


if __name__ == '__main__':
    unittest.main()