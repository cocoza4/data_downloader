
import unittest as ut
# from unittest.mock import patch
#
# from data_downloader import data_downloader
#
#
class InterviewTest(ut.TestCase):

    def test(self):
        def reverse(arr):
            for i in range(len(arr) // 2):
                arr[i], arr[len(arr) - i - 1] = arr[len(arr) - i - 1], arr[i]

            return arr

        print(reverse([1, 2, 3]))

        def reverse_2(arr):
            if len(arr) == 0:
                return []
            else:
                first = arr[0]
                return reverse(arr[1:]) ++ [first]

        print(reverse_2([1, 2, 3]))

#     def test_question_1(self):
#         inp = '11222551'
#         actual = data_downloader.question_1(inp)
#         exp = [(2, 1), (3, 2), (2, 5), (1, 1)]
#         self.assertEqual(exp, actual)
#
#     def test_binary_search(self):
#         inp = [1, 2, 3, 4, 5, 6, 7, 8, 8]
#         self.assertEqual(data_downloader.binary_search(inp, 7), 7)
#         self.assertEqual(data_downloader.binary_search(inp, 1), 1)
#         self.assertEqual(data_downloader.binary_search(inp, 8), 8)
#         self.assertIsNone(data_downloader.binary_search(inp, 0))

