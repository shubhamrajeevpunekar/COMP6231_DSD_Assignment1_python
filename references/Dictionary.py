# class Dictionary:
#
#     def __init__(self):
#         self.dictionary = dict()
#
#     def add(self, key, num):
#         self.dictionary[key] = num
#         return num
#
#     def set(self, key, num):
#         if key not in self.dictionary:
#             return "Error! Key not found for: " + key
#         del self.dictionary[key]
#         self.dictionary[key] = num
#         return num
#
#     def delete(self, key):
#         if key not in self.dictionary:
#             return "Error! Key not found for: " + key
#         del self.dictionary[key]
#         return ''
#
#     def list(self):
#         return list(self.dictionary).__str__()
#
#     def get(self, key):
#         if key not in self.dictionary:
#             return "Error! Key not found for: " + key
#         return self.dictionary[key]
#
#     def getValues(self, key):
#         if key not in self.dictionary:
#             return "Error! Key not found for: " + key
#
#         numbers = ''
#         for k, num in self.dictionary:
#             if k == key:
#                 numbers += num + ', '
#         numbers.strip()
#         return numbers[:-1]
#
#     def reset(self):
#         self.dictionary.clear()
#
#     def max(self, key):
#         if key not in self.dictionary:
#             return "Error! Key not found for: " + key
#
#         maxNum = self.dictionary[key]
#         for k, num in self.dictionary:
#             if k == key and num > maxNum:
#                 maxNum = num
#         return maxNum
#
#     def min(self, key):
#         if key not in self.dictionary.keys():
#             return "Error! Key not found for: " + key
#
#         minNum = self.dictionary[key]
#         for k, num in self.dictionary:
#             if k == key and num < minNum:
#                 minNum = num
#         return minNum
#
#     def sum(self, key):
#         if key not in self.dictionary:
#             return "Error! Key not found for: " + key
#
#         sums = 0
#         for k, num in self.dictionary:
#             if k == key:
#                 sums += num
#         return sums
#
#     def avg(self, key):
#         if key not in self.dictionary:
#             return "Error! Key not found for: " + key
#
#         avg, count = 0, 0
#         for k, num in self.dictionary:
#             if k == key:
#                 avg += num
#                 count += 1
#         return avg/count
