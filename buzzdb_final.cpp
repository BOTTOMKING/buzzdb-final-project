#include <iostream>
#include <memory>
#include <cstring>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <cassert>

constexpr size_t PAGE_SIZE = 4096;
constexpr size_t MAX_SLOTS = 512;
constexpr uint16_t INVALID_VALUE = std::numeric_limits<uint16_t>::max();

struct Slot {
    bool empty = true;
    uint16_t offset = INVALID_VALUE;
    uint16_t length = INVALID_VALUE;
};

class Field {
public:
    int value;
    Field(int val) : value(val) {}
    int getValue() const { return value; }
};

class Tuple {
public:
    std::vector<std::unique_ptr<Field>> fields;

    void addField(std::unique_ptr<Field> field) {
        fields.push_back(std::move(field));
    }

    size_t getSize() const {
        return sizeof(int) * fields.size();
    }

    std::string serialize() const {
        std::stringstream buffer;
        for (const auto& field : fields) {
            buffer << field->getValue() << " ";
        }
        return buffer.str();
    }

    static std::unique_ptr<Tuple> deserialize(const std::string& data) {
        std::stringstream ss(data);
        auto tuple = std::make_unique<Tuple>();
        int value;
        while (ss >> value) {
            tuple->addField(std::make_unique<Field>(value));
        }
        return tuple;
    }

    void print() const {
        for (const auto& field : fields) {
            std::cout << field->getValue() << " ";
        }
        std::cout << "\n";
    }
};

class SlottedPage {
public:
    std::unique_ptr<char[]> page_data = std::make_unique<char[]>(PAGE_SIZE);
    size_t metadata_size = sizeof(Slot) * MAX_SLOTS;

    SlottedPage() {
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t i = 0; i < MAX_SLOTS; i++) {
            slot_array[i].empty = true;
            slot_array[i].offset = INVALID_VALUE;
            slot_array[i].length = INVALID_VALUE;
        }
    }

    bool addTuple(std::unique_ptr<Tuple> tuple) {
        size_t tuple_size = tuple->getSize();
        auto serialized_tuple = tuple->serialize();
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());

        for (size_t i = 0; i < MAX_SLOTS; i++) {
            if (slot_array[i].empty) {
                size_t offset = (i == 0) ? metadata_size : slot_array[i - 1].offset + slot_array[i - 1].length;
                if (offset + tuple_size >= PAGE_SIZE) return false;

                std::memcpy(page_data.get() + offset, serialized_tuple.c_str(), tuple_size);
                slot_array[i].empty = false;
                slot_array[i].offset = offset;
                slot_array[i].length = tuple_size;
                return true;
            }
        }
        return false;
    }

    void deleteTuple(size_t index) {
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        if (index < MAX_SLOTS && !slot_array[index].empty) {
            slot_array[index].empty = true;
        }
    }

    void compactPage() {
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        size_t new_offset = metadata_size;
        for (size_t i = 0; i < MAX_SLOTS; i++) {
            if (!slot_array[i].empty) {
                uint16_t old_offset = slot_array[i].offset;
                uint16_t length = slot_array[i].length;

                if (old_offset != new_offset) {
                    std::memmove(page_data.get() + new_offset, page_data.get() + old_offset, length);
                    slot_array[i].offset = new_offset;
                }

                new_offset += length;
            }
        }
    }

    void print() const {
        Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
        for (size_t i = 0; i < MAX_SLOTS; i++) {
            if (!slot_array[i].empty) {
                const char* tuple_data = page_data.get() + slot_array[i].offset;
                std::istringstream iss(std::string(tuple_data, slot_array[i].length));
                auto tuple = Tuple::deserialize(iss.str());
                std::cout << "Slot " << i << ": ";
                tuple->print();
            }
        }
    }
};

class StorageManager {
public:
    std::unordered_map<uint16_t, SlottedPage> pages;

    SlottedPage& load(uint16_t page_id) {
        return pages[page_id];
    }
};

class BufferManager {
private:
    StorageManager storage_manager;

public:
    StorageManager& getStorageManager() {
        return storage_manager;
    }

    bool moveTupleAcrossPages(uint16_t from_page_id, uint16_t to_page_id) {
        SlottedPage& from_page = storage_manager.load(from_page_id);
        SlottedPage& to_page = storage_manager.load(to_page_id);

        Slot* from_slots = reinterpret_cast<Slot*>(from_page.page_data.get());
        Slot* to_slots = reinterpret_cast<Slot*>(to_page.page_data.get());

        for (size_t i = 0; i < MAX_SLOTS; i++) {
            if (!from_slots[i].empty) {
                uint16_t length = from_slots[i].length;
                size_t new_offset = to_page.metadata_size;

                if (new_offset + length >= PAGE_SIZE) return false;

                std::memmove(to_page.page_data.get() + new_offset, from_page.page_data.get() + from_slots[i].offset, length);
                from_slots[i].empty = true;

                size_t j = 0;
                for (; j < MAX_SLOTS; j++) {
                    if (to_slots[j].empty) break;
                }

                to_slots[j].empty = false;
                to_slots[j].offset = new_offset;
                to_slots[j].length = length;

                to_page.metadata_size += length;
            }
        }

        from_page.compactPage();
        return true;
    }
};

int main() {
    BufferManager bufferManager;
    auto& page1 = bufferManager.getStorageManager().load(0);
    auto& page2 = bufferManager.getStorageManager().load(1);

    auto tuple1 = std::make_unique<Tuple>();
    tuple1->addField(std::make_unique<Field>(10));
    page1.addTuple(std::move(tuple1));

    auto tuple2 = std::make_unique<Tuple>();
    tuple2->addField(std::make_unique<Field>(20));
    page1.addTuple(std::move(tuple2));

    std::cout << "Before compaction:\n";
    page1.print();

    page1.deleteTuple(0);
    page1.compactPage();

    std::cout << "After compaction:\n";
    page1.print();

    std::cout << "Moving tuple across pages:\n";
    bufferManager.moveTupleAcrossPages(0, 1);
    page2.print();

    return 0;
}
