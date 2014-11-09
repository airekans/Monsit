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

    def test_len(self):
        self.assertEqual(len(self.array), len(self.queue))

    def test_reset(self):
        self.assertGreater(self.queue.size(), 0)
        self.queue.reset()
        self.assertEqual(0, self.queue.size())

        new_array = [3, 2, 1, 4]
        self.queue.reset(new_array)
        self.assertEqual(4, self.queue.size())
        self.assertEqual(len(new_array), self.queue.size())
        self.assert_heapify(new_array, 0)

    def test_is_empty(self):
        self.assertGreater(len(self.queue), 0)
        self.assertFalse(self.queue.is_empty())

        self.queue.reset([])
        self.assertEqual(0, self.queue.size())
        self.assertTrue(self.queue.is_empty())

    def test_push(self):
        old_queue_size = self.queue.size()
        self.queue.push(2)
        self.assertEqual(old_queue_size + 1, self.queue.size())

        self.assert_heapify(self.array, 0)

    def test_pop(self):
        old_queue_size = self.queue.size()
        self.assertEqual(1, self.queue.pop())
        self.assertEqual(old_queue_size - 1, self.queue.size())

    def test_get(self):
        for index in xrange(len(self.queue)):
            self.assertEqual(self.array[index], self.queue.get(index))
            self.assertEqual(self.array[index], self.queue[index])

    def test_increase_key(self):
        key_index = self.queue.find(5)
        self.assertGreaterEqual(self.queue.increase_key(key_index, 9), key_index)

        new_index = self.queue.find(9)
        self.assertNotEqual(-1, new_index)
        self.assertEqual(9, self.array[new_index])
        self.assert_heapify(self.array, 0)

    def test_decrease_key(self):
        key_index = self.queue.find(5)
        self.assertLessEqual(self.queue.decrease_key(key_index, 0), key_index)

        new_index = self.queue.find(0)
        self.assertNotEqual(-1, new_index)
        self.assertEqual(0, new_index)
        self.assertEqual(0, self.array[new_index])
        self.assert_heapify(self.array, 0)


if __name__ == '__main__':
    unittest.main()
