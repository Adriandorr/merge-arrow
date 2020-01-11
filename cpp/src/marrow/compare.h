//
// Created by adorr on 11/01/2020.
//

#ifndef CPP_COMPARE_H
#define CPP_COMPARE_H

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <marrow/string_array.h>

namespace marrow {
    class IComparer {
    public:
        virtual bool operator()(int64_t index1, int64_t index2) const = 0;
    };

    template<typename TArray = arrow::Int32Array>
    class SimpleComparer : public IComparer {
    public:
        SimpleComparer(std::shared_ptr<arrow::Array> array) : _array(std::static_pointer_cast<TArray>(array)) {

        }
        bool operator()(int64_t index1, int64_t index2) const final {
            auto v1 = _array->Value(index1);
            auto v2 = _array->Value(index2);
            return v1 < v2;
        }
        private:
            std::shared_ptr<TArray> _array;
    };

    class StringComparer : public IComparer {
    public:
        StringComparer(std::shared_ptr<IStringArray> array) :  _array(array) {

        }
        bool operator()(int64_t index1, int64_t index2) const final {
            return _array->Value(index1) < _array->Value(index2);
        }
    private:
        std::shared_ptr<IStringArray> _array;
    };

    class NullComparer : public IComparer {
    public:
        NullComparer(std::shared_ptr<arrow::Array> array, std::shared_ptr<IComparer> comparer) : _array(array), _comparer(comparer) {
        }

        bool operator()(int64_t index1, int64_t index2) const final {
            if (_array->IsNull(index1)) {
                return !_array->IsNull(index2);
            }
            if (_array->IsNull(index2)) {
                return false;
            }
            return (*_comparer)(index1, index2);
        }

    private:
        std::shared_ptr<arrow::Array> _array;
        std::shared_ptr<IComparer> _comparer;
    };

    static inline std::shared_ptr<IComparer> make_array_comparer(std::shared_ptr<arrow::Array> array) {
        std::shared_ptr<IComparer> ret;
        switch (array->type_id()) {
            case arrow::Type::INT8:
                ret = std::make_shared<SimpleComparer<arrow::Int8Array>>(array);
                break;
            case arrow::Type::INT16:
                ret = std::make_shared<SimpleComparer<arrow::Int16Array>>(array);
                break;
            case arrow::Type::INT32:
                ret = std::make_shared<SimpleComparer<arrow::Int32Array>>(array);
                break;
            case arrow::Type::INT64:
                ret = std::make_shared<SimpleComparer<arrow::Int64Array>>(array);
                break;
            case arrow::Type::UINT8:
                ret = std::make_shared<SimpleComparer<arrow::UInt8Array>>(array);
                break;
            case arrow::Type::UINT16:
                ret = std::make_shared<SimpleComparer<arrow::UInt16Array>>(array);
                break;
            case arrow::Type::UINT32:
                ret = std::make_shared<SimpleComparer<arrow::UInt32Array>>(array);
                break;
            case arrow::Type::UINT64:
                ret = std::make_shared<SimpleComparer<arrow::UInt64Array>>(array);
                break;
            case arrow::Type::HALF_FLOAT:
                ret = std::make_shared<SimpleComparer<arrow::HalfFloatArray>>(array);
                break;
            case arrow::Type::FLOAT:
                ret = std::make_shared<SimpleComparer<arrow::FloatArray>>(array);
                break;
            case arrow::Type::DOUBLE:
                ret = std::make_shared<SimpleComparer<arrow::DoubleArray>>(array);
                break;
            case arrow::Type::STRING:
            case arrow::Type::LARGE_STRING:
            case arrow::Type::DICTIONARY:
                ret = std::make_shared<StringComparer>(make_istring_array(array));
                break;
            default:
                throw std::runtime_error("Unsupported array type for compare: " + array->type()->ToString());
        }
        if (array->null_count() != 0) {
            ret = std::make_shared<NullComparer>(array, ret);
        }
        return ret;
    }

    class ColumnsComparer : public IComparer {
    public:
        ColumnsComparer(std::vector<std::shared_ptr<IComparer>> compairer) : _compairer(compairer) {

        }
        bool operator()(int64_t index1, int64_t index2) const final {
            for (auto c: _compairer) {
                if ((*c)(index1, index2)) {
                    return true;
                }
                if ((*c)(index2, index1)) {
                    return false;
                }
            }
            return false;
        }
    private:
        std::vector<std::shared_ptr<IComparer>> _compairer;
    };

    static inline std::shared_ptr<IComparer> make_comparer(std::shared_ptr<arrow::RecordBatch> batch, std::vector<std::string> columns) {
        std::vector<std::shared_ptr<IComparer>> compairer;
        for (auto& c: columns) {
            auto array = batch->GetColumnByName(c);
            if (!array) {
                throw std::runtime_error("Column missing: " + c);
            }
            compairer.push_back(make_array_comparer(array));
        }
        if (compairer.size() == 1) {
            return compairer[0];
        }
        return std::make_shared<ColumnsComparer>(compairer);
    }
}
#endif //CPP_COMPARE_H
