import heapq


class PriorityQueue(object):
    """ This is the min queue by default
    """

    def __init__(self, initial_array=[]):
        self.__array = initial_array
        heapq.heapify(self.__array)  # make the heap

    @staticmethod
    def left_child(parent_i):
        return parent_i * 2 + 1

    @staticmethod
    def right_child(parent_i):
        return parent_i * 2 + 2

    @staticmethod
    def parent(child_i):
        return (child_i - 1) / 2

    def reset(self, new_array=[]):
        self.__array = new_array
        heapq.heapify(self.__array)

    def is_empty(self):
        return len(self.__array) == 0

    def size(self):
        return len(self.__array)

    def __len__(self):
        return self.size()

    def get(self, index):
        return self.__array[index]

    def __getitem__(self, item):
        return self.get(item)

    def get_top(self):
        return self.__array[0]

    def push(self, elem):
        heapq.heappush(self.__array, elem)

    def pop(self):
        return heapq.heappop(self.__array)

    # TODO: there is another more efficient solution for this
    #       by using the heap property
    def find(self, elem):
        try:
            return self.__array.index(elem)
        except ValueError:
            return -1

    def _swap(self, i1, i2):
        self.__array[i1], self.__array[i2] = self.__array[i2], self.__array[i1]

    def decrease_key(self, index, elem):
        if elem >= self.__array[index]:
            return -1

        self.__array[index] = elem
        parent_i = PriorityQueue.parent(index)
        while index > 0 and self.__array[parent_i] > self.__array[index]:
            self._swap(index, parent_i)
            index, parent_i = parent_i, PriorityQueue.parent(parent_i)

        return index

    def increase_key(self, index, elem):
        if elem <= self.__array[index]:
            return -1

        self.__array[index] = elem
        queue_size = self.size()
        left_i = PriorityQueue.left_child(index)
        right_i = PriorityQueue.right_child(index)
        while left_i < queue_size:
            min_i = index
            if self.__array[left_i] < self.__array[min_i]:
                min_i = left_i

            if right_i < queue_size and \
                    self.__array[right_i] < self.__array[min_i]:
                min_i = right_i

            if min_i == index:
                break

            self._swap(index, min_i)
            index = min_i
            left_i = PriorityQueue.left_child(index)
            right_i = PriorityQueue.right_child(index)

        return index

