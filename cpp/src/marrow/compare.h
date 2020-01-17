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
        virtual bool lt(int64_t index1, int64_t index2) const = 0;
        virtual bool gt(int64_t index1, int64_t index2) const = 0;
    };

    template<typename TArray = arrow::Int32Array>
    class SimpleComparer : public IComparer {
    public:
        SimpleComparer(std::shared_ptr<arrow::Array> array1, std::shared_ptr<arrow::Array> array2) : _array1(std::static_pointer_cast<TArray>(array1)), _array2(std::static_pointer_cast<TArray>(array2)) {
        }

        bool lt(int64_t index1, int64_t index2) const final {
            auto v1 = _array1->Value(index1);
            auto v2 = _array2->Value(index2);
            return v1 < v2;
        }
        bool gt(int64_t index1, int64_t index2) const final {
            auto v1 = _array1->Value(index1);
            auto v2 = _array2->Value(index2);
            return v1 > v2;
        }
        private:
            std::shared_ptr<TArray> _array1, _array2;
    };

    class StringComparer : public IComparer {
    public:
        StringComparer(std::shared_ptr<IStringArray> array1, std::shared_ptr<IStringArray> array2) :  _array1(array1), _array2(array2) {

        }
        bool lt(int64_t index1, int64_t index2) const final {
            return _array1->Value(index1) < _array2->Value(index2);
        }
        bool gt(int64_t index1, int64_t index2) const final {
            return _array1->Value(index1) > _array2->Value(index2);
        }
    private:
        std::shared_ptr<IStringArray> _array1, _array2;
    };

    class NullComparer : public IComparer {
    public:
        NullComparer(std::shared_ptr<arrow::Array> array1, std::shared_ptr<arrow::Array> array2, std::shared_ptr<IComparer> comparer) : _array1(array1), _array2(array2), _comparer(comparer) {
        }

        bool lt(int64_t index1, int64_t index2) const final {
            if (_array1->IsNull(index1)) {
                return !_array2->IsNull(index2);
            }
            if (_array2->IsNull(index2)) {
                return false;
            }
            return _comparer->lt(index1, index2);
        }

        bool gt(int64_t index1, int64_t index2) const final {
            if (_array1->IsNull(index1)) {
                return false;
            }
            if (_array2->IsNull(index2)) {
                return true;
            }
            return _comparer->gt(index1, index2);
        }

    private:
        std::shared_ptr<arrow::Array> _array1, _array2;
        std::shared_ptr<IComparer> _comparer;
    };

    static inline std::shared_ptr<IComparer> make_array_comparer(std::shared_ptr<arrow::Array> array1, std::shared_ptr<arrow::Array> array2) {
        std::shared_ptr<IComparer> ret;
        if (!array1->type()->Equals(array2->type())) {
            throw std::runtime_error("Incompatible array tipes for compare on columns: " + array1->type()->ToString() + " != " + array2->type()->ToString());
        }
        switch (array1->type_id()) {
            case arrow::Type::INT8:
                ret = std::make_shared<SimpleComparer<arrow::Int8Array>>(array1, array2);
                break;
            case arrow::Type::INT16:
                ret = std::make_shared<SimpleComparer<arrow::Int16Array>>(array1, array2);
                break;
            case arrow::Type::INT32:
                ret = std::make_shared<SimpleComparer<arrow::Int32Array>>(array1, array2);
                break;
            case arrow::Type::INT64:
                ret = std::make_shared<SimpleComparer<arrow::Int64Array>>(array1, array2);
                break;
            case arrow::Type::UINT8:
                ret = std::make_shared<SimpleComparer<arrow::UInt8Array>>(array1, array2);
                break;
            case arrow::Type::UINT16:
                ret = std::make_shared<SimpleComparer<arrow::UInt16Array>>(array1, array2);
                break;
            case arrow::Type::UINT32:
                ret = std::make_shared<SimpleComparer<arrow::UInt32Array>>(array1, array2);
                break;
            case arrow::Type::UINT64:
                ret = std::make_shared<SimpleComparer<arrow::UInt64Array>>(array1, array2);
                break;
            case arrow::Type::HALF_FLOAT:
                ret = std::make_shared<SimpleComparer<arrow::HalfFloatArray>>(array1, array2);
                break;
            case arrow::Type::FLOAT:
                ret = std::make_shared<SimpleComparer<arrow::FloatArray>>(array1, array2);
                break;
            case arrow::Type::DOUBLE:
                ret = std::make_shared<SimpleComparer<arrow::DoubleArray>>(array1, array2);
                break;
            case arrow::Type::STRING:
            case arrow::Type::LARGE_STRING:
            case arrow::Type::DICTIONARY: {
                auto string_array1 = make_istring_array(array1);
                auto string_array2 = make_istring_array(array2);
                ret = std::make_shared<StringComparer>(string_array1, string_array2);
            }
                break;
            default:
                throw std::runtime_error("Unsupported array type for compare: " + array1->type()->ToString());
        }
        if (array1->null_count() != 0 || array2->null_count() != 0) {
            ret = std::make_shared<NullComparer>(array1, array2, ret);
        }
        return ret;
    }

    class ColumnsComparer : public IComparer {
    public:
        ColumnsComparer(std::vector<std::shared_ptr<IComparer>> comparers) : _comparers(comparers) {

        }
        bool lt(int64_t index1, int64_t index2) const final {
            for (auto c: _comparers) {
                if (c->lt(index1, index2)) {
                    return true;
                }
                if (c->gt(index2, index1)) {
                    return false;
                }
            }
            return false;
        }
        bool gt(int64_t index1, int64_t index2) const final {
            for (auto c: _comparers) {
                if (c->gt(index1, index2)) {
                    return true;
                }
                if (c->lt(index1, index2)) {
                    return false;
                }
            }
            return false;
        }
    private:
        std::vector<std::shared_ptr<IComparer>> _comparers;
    };

    static inline std::shared_ptr<IComparer> make_comparer(std::shared_ptr<arrow::RecordBatch> batch1, std::shared_ptr<arrow::RecordBatch> batch2, std::vector<std::string> columns) {
        std::vector<std::shared_ptr<IComparer>> comparer;
        for (auto& c: columns) {
            auto array1 = batch1->GetColumnByName(c);
            if (!array1) {
                throw std::runtime_error("Column missing from batch1: " + c);
            }
            auto array2 = batch2->GetColumnByName(c);
            if (!array2) {
                throw std::runtime_error("Column missing from batch2: " + c);
            }
            comparer.push_back(make_array_comparer(array1, array2));
        }
        if (comparer.size() == 1) {
            return comparer[0];
        }
        return std::make_shared<ColumnsComparer>(comparer);
    }

    static inline std::shared_ptr<IComparer> make_comparer(std::shared_ptr<arrow::RecordBatch> batch, std::vector<std::string> columns) {
        return make_comparer(batch, batch, std::move(columns));
    }
}
#endif //CPP_COMPARE_H
