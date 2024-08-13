#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  auto cur{root_};
  for (auto ch : key) {
    if (cur == nullptr || cur->children_.find(ch) == cur->children_.end()) {
      return nullptr;
    }
    cur = cur->children_.find(ch)->second;
  }
  if (auto *sptr = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get()); sptr) {
    return static_cast<const T *>(sptr->value_.get());
  }
  return nullptr;
}

template <class T>
auto Trie::PutDfs(std::string_view key, int pos, const std::shared_ptr<const TrieNode> &cur, T value) const
    -> std::shared_ptr<const TrieNode> {
  // 拷贝节点
  std::shared_ptr<TrieNode> new_cur = (cur == nullptr) ? std::make_shared<TrieNode>() : cur->Clone();

  if (pos == static_cast<int>(key.size())) {
    return std::make_shared<const TrieNodeWithValue<T>>(new_cur->children_, std::make_shared<T>(std::move(value)));
  }

  if (cur == nullptr || cur->children_.find(key[pos]) == cur->children_.end()) {  // 不存在路径
    new_cur->children_[key[pos]] = PutDfs(key, pos + 1, nullptr, std::move(value));
  } else {  // 存在路径
    new_cur->children_[key[pos]] = PutDfs(key, pos + 1, cur->children_.find(key[pos])->second, std::move(value));
  }
  return new_cur;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  return Trie(PutDfs(key, 0, root_, std::move(value)));
}

auto Trie::RemoveDfs(std::string_view key, int pos, const std::shared_ptr<const TrieNode> &cur) const
    -> std::shared_ptr<const TrieNode> {
  // 拷贝节点
  std::shared_ptr<TrieNode> new_cur{cur->Clone()};
  if (pos == static_cast<int>(key.size())) {
    return (!new_cur->children_.empty()) ? std::make_shared<const TrieNode>(new_cur->children_) : nullptr;
  }
  // if(cur == nullptr or cur->children_.find(key[pos]) == cur->children_.end())
  //   return cur; // 没有对应的 key，就什么都不做
  auto tmp = RemoveDfs(key, pos + 1, cur->children_.find(key[pos])->second);
  if (tmp) {
    new_cur->children_[key[pos]] = tmp;
  } else {
    new_cur->children_.erase(key[pos]);
  }
  if (new_cur->is_value_node_ || (!new_cur->children_.empty())) {
    return new_cur;
  }
  return nullptr;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  return Trie(RemoveDfs(key, 0, root_));
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the`
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;
template auto Trie::PutDfs(std::string_view key, int pos, const std::shared_ptr<const TrieNode> &cur,
                           uint32_t value) const -> std::shared_ptr<const TrieNode>;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;
template auto Trie::PutDfs(std::string_view key, int pos, const std::shared_ptr<const TrieNode> &cur,
                           uint64_t value) const -> std::shared_ptr<const TrieNode>;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;
template auto Trie::PutDfs(std::string_view key, int pos, const std::shared_ptr<const TrieNode> &cur,
                           std::string value) const -> std::shared_ptr<const TrieNode>;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;
template auto Trie::PutDfs(std::string_view key, int pos, const std::shared_ptr<const TrieNode> &cur,
                           Integer value) const -> std::shared_ptr<const TrieNode>;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;
template auto Trie::PutDfs(std::string_view key, int pos, const std::shared_ptr<const TrieNode> &cur,
                           MoveBlocked value) const -> std::shared_ptr<const TrieNode>;

}  // namespace bustub
