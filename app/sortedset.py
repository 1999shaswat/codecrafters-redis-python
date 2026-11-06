from random import random


class Sortedset:
    def __init__(self):
        self.list = Skiplist()


class Skipnode:
    def __init__(self, key, score, level):
        self.key = key
        self.score = score
        self.forward = [None] * level
        self.span = [0] * level


class Skiplist:
    def __init__(self):
        self.maxlvl = 16
        self.head = Skipnode(None, None, self.maxlvl)
        self.lvl = 1
        self.length = 0

    def gen_lvl(self):
        lvl = 1
        while lvl < self.maxlvl and random() < 0.5:
            lvl += 1
        return lvl

    def less(self, sa, ka, sb, kb):
        return (sa, ka) < (sb, kb)

    def insert(self, key, score):
        curr = self.head
        update = [None] * self.maxlvl
        rank = [0] * self.maxlvl

        for l in reversed(range(self.lvl)):
            if l < self.lvl - 1:
                rank[l] = rank[l + 1]
            while curr.forward[l] is not None and self.less(
                curr.forward[l].score, curr.forward[l].key, score, key
            ):
                rank[l] += curr.span[l]
                curr = curr.forward[l]
            update[l] = curr

        new_lvl = self.gen_lvl()
        node = Skipnode(key, score, new_lvl)
        if new_lvl > self.lvl:
            for l in range(self.lvl, new_lvl):
                update[l] = self.head
                rank[l] = 0
            self.lvl = new_lvl

        for l in range(new_lvl):
            node.forward[l] = update[l].forward[l]
            update[l].forward[l] = node

            node.span[l] = update[l].span[l] - (rank[0] - rank[l])
            update[l].span[l] = (rank[0] - rank[l]) + 1

        for l in range(new_lvl, self.lvl):
            update[l].span[l] += 1
        self.length += 1

    def remove(self, key, score):
        curr = self.head
        update = [None] * self.maxlvl

        for l in reversed(range(self.lvl)):
            while curr.forward[l] is not None and self.less(
                curr.forward[l].score, curr.forward[l].key, score, key
            ):
                curr = curr.forward[l]
            update[l] = curr

        curr = curr.forward[0]
        if curr is None or curr.score != score or curr.key != key:
            return False

        for l in range(self.lvl):
            if update[l].forward[l] == curr:
                update[l].span[l] += curr.span[l] - 1
                update[l].forward[l] = curr.forward[l]
            else:
                update[l].span[l] -= 1

        while self.lvl > 1 and self.head.forward[self.lvl - 1] is None:
            self.lvl -= 1

        self.length -= 1
        return True

    def get_rank(self, key, score):
        curr = self.head
        rank = 0
        for l in reversed(range(self.lvl)):
            while curr.forward[l] is not None and self.less(
                curr.forward[l].score, curr.forward[l].key, score, key
            ):
                rank += curr.span[l]
                curr = curr.forward[l]

        curr = curr.forward[0]
        if curr is None or curr.score != score or curr.key != key:
            return -1
        return rank

    def get_range(self, start, end):
        if start < 0:
            start = 0
        if end >= self.length:
            end = self.length - 1
        if start > end or start >= self.length:
            return []

        curr = self.head
        traversed = 0

        for l in reversed(range(self.lvl)):
            while curr.forward[l] is not None and traversed + curr.span[l] <= start:
                traversed += curr.span[l]
                curr = curr.forward[l]
        if curr.forward[0] is None:
            return []
        curr = curr.forward[0]
        res = []
        idx = start

        while curr is not None and idx <= end:
            res.append((curr.key, curr.score))
            curr = curr.forward[0]
            idx += 1
        return res
