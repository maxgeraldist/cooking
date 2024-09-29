from collections import defaultdict


# Define a TrieNode class to represent a node in the trie
class TrieNode:
    def __init__(self):
        self.children = defaultdict(TrieNode)
        self.is_end_of_word = False
        self.index = None


# Define a Trie class to represent the trie data structure
class Trie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, word, index):
        current_node = self.root
        for char in word:
            current_node = current_node.children[char]
        current_node.is_end_of_word = True
        current_node.index = index

    def search_longest_substring(self, word):
        current_node = self.root
        longest_substring_index = None
        longest_substring_length = 0
        for i in range(len(word)):
            current_node = self.root
            substring_length = 0
            for char in word[i:]:
                if char not in current_node.children:
                    break
                current_node = current_node.children[char]
                substring_length += 1
                if (
                    current_node.is_end_of_word
                    and substring_length > longest_substring_length
                ):
                    longest_substring_index = current_node.index
                    longest_substring_length = substring_length
        return longest_substring_index

    def search(self, word):
        current_node = self.root
        for char in word:
            if char not in current_node.children:
                return False
            current_node = current_node.children[char]
        return current_node.index


popular_ingredients_trie = Trie()
n = 3


# Define a function to replace the IDs of unpopular ingredients
def replace_IDs(row):
    if row["ingredientcount"] <= n:
        # Find the longest popular ingredient whose name is a substring of the current ingredient's name
        longest_substring_index = popular_ingredients_trie.search_longest_substring(
            row["ingredient"].replace("[:,.()]", "")
        )
        if longest_substring_index is not None:
            return longest_substring_index
