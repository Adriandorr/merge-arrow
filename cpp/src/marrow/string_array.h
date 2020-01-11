//
// Created by adorr on 11/01/2020.
//

#ifndef MARROW_STRING_ARRAY_H
#define MARROW_STRING_ARRAY_H

#include <arrow/array.h>

namespace marrow {
    class IStringArray {
    public:
        virtual ~IStringArray() = default;

        virtual arrow::util::string_view Value(int64_t index) const = 0;

        virtual const arrow::Array& array() const = 0;
    };

    template<typename TArray = arrow::Int32Array>
    class StringDictionaryArray : public IStringArray {
    public:
        StringDictionaryArray(std::shared_ptr<TArray> indices, std::shared_ptr<arrow::StringArray> dict) : _indices(indices), _dict(dict) {}

        arrow::util::string_view Value(int64_t index) const final {
            index = _indices->Value(index);
            auto value = _dict->GetView(index);
            return value;
        }

        const arrow::Array& array() const final {
            return *_indices;
        }
    private:
        std::shared_ptr<TArray> _indices;
        std::shared_ptr<arrow::StringArray> _dict;

    };

    template<typename TArray = arrow::StringArray>
    class StringArray : public IStringArray {
    public:
        StringArray(std::shared_ptr<arrow::Array> array) : _array(std::static_pointer_cast<TArray>(array)) {}

        arrow::util::string_view Value(int64_t index) const final {
            auto value = _array->GetView(index);
            return value;
        }

        const arrow::Array& array() const final {
            return *_array;
        }
    private:
        std::shared_ptr<TArray> _array;

    };

    std::shared_ptr<IStringArray> make_istring_array(std::shared_ptr<arrow::Array> array) {
        switch (array->type_id()) {
            case arrow::Type::STRING:
                return std::make_shared<StringArray<arrow::StringArray>>(array);
            case arrow::Type::LARGE_STRING:
                return std::make_shared<StringArray<arrow::LargeStringArray>>(array);
            case arrow::Type::DICTIONARY: {
                auto dict_array = std::static_pointer_cast<arrow::DictionaryArray>(array);
                if (dict_array->dictionary()->type_id() != arrow::Type::STRING) {
                    throw std::runtime_error("Dictionary is not a string type: " + dict_array->dictionary()->type()->ToString());
                }
                auto indices = dict_array->indices();
                auto dict = std::static_pointer_cast<arrow::StringArray>(dict_array->dictionary());
                switch (indices->type_id()) {
                    case arrow::Type::INT8:
                        return std::make_shared<StringDictionaryArray<arrow::Int8Array>>(std::static_pointer_cast<arrow::Int8Array>(indices), dict);
                    case arrow::Type::INT16:
                        return std::make_shared<StringDictionaryArray<arrow::Int16Array>>(std::static_pointer_cast<arrow::Int16Array>(indices), dict);
                    case arrow::Type::INT32:
                        return std::make_shared<StringDictionaryArray<arrow::Int32Array>>(std::static_pointer_cast<arrow::Int32Array>(indices), dict);
                    default:
                        throw std::runtime_error("Indices have invalid type" + indices->type()->ToString());
                }
            }
            default:
                throw std::runtime_error("Invalid array to create IStringArray: " + array->type()->ToString());
        }
    }
}

#endif //MARROW_STRING_ARRAY_H
