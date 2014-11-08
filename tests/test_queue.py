from monsit import queue
import unittest


class PriorityQueueTest(unittest.TestCase):

    def setUp(self):
        self.array = [7, 4, 2, 6, 3, 5, 1, 8, 7]
        self.queue = queue.PriorityQueue(self.array)

    def assert_heapify(self, array, index):
        queue_size = len(array)
        left_i = queue.PriorityQueue.left_child(index)
        right_i = queue.PriorityQueue.right_child(index)
        if left_i < queue_size:
            self.assertLessEqual(array[index], array[left_i])
            self.assert_heapify(array, left_i)

            if right_i < queue_size:
                self.assertLessEqual(array[index], array[right_i])
                self.assert_heapify(array, right_i)

    def test_heapify(self):
        self.assertEqual(len(self.array), self.queue.size())
        self.assertEqual(1, self.array[0])
        self.assertEqual(1, self.queue.get_top())

        print self.array

        self.assert_heapify(self.array, 0)

    def test_pop(self):
        old_queue_size = self.queue.size()
        self.assertEqual(1, self.queue.pop())
        self.assertEqual(old_queue_size - 1, self.queue.size())

    def test_increase_key(self):
        key_index = self.queue.find(5)
        self.queue.increase_key(key_index, 9)

        new_index = self.queue.find(9)
        self.assertNotEqual(-1, new_index)
        self.assertEqual(9, self.array[new_index])
        self.assert_heapify(self.array, 0)

    def test_decrease_key(self):
        key_index = self.queue.find(5)
        self.queue.decrease_key(key_index, 0)

        new_index = self.queue.find(0)
        self.assertNotEqual(-1, new_index)
        self.assertEqual(0, new_index)
        self.assertEqual(0, self.array[new_index])
        self.assert_heapify(self.array, 0)


if __name__ == '__main__':
    unittest.main()
