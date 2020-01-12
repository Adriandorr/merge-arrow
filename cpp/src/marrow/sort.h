//
// Created by adorr on 11/01/2020.
//

#ifndef MARROW_SORT_H
#define MARROW_SORT_H

#include <utility>
#include "marrow/index.h"

namespace marrow {

    class IColumnBuilder {
    public:
        virtual ~IColumnBuilder() = default;

        virtual arrow::Status Reserve(int64_t size) = 0;
        virtual arrow::Status Append(int64_t source_index) = 0;
        virtual std::shared_ptr<arrow::Array> Finish() = 0;
    };

    template<typename TType>
    class NumericColumnBuilder : public IColumnBuilder {
    public:
        explicit NumericColumnBuilder(std::shared_ptr<arrow::Array> array) : _array(std::static_pointer_cast<typename arrow::TypeTraits<TType>::ArrayType>(array)) {
        }

        arrow::Status Reserve(int64_t size) final {
            return _builder.Reserve(size);
        }

        arrow::Status Append(int64_t source_index) final {
            if (_array->IsNull(source_index)) {
                return _builder.AppendNull();
            }
            auto v = _array->Value(source_index);
            return _builder.Append(v);
        }

        std::shared_ptr<arrow::Array> Finish() final {
            std::shared_ptr<arrow::Array> ret;
            auto status = _builder.Finish(&ret);
            return ret;
        }

    private:
        std::shared_ptr<typename arrow::TypeTraits<TType>::ArrayType> _array;
        typename arrow::TypeTraits<TType>::BuilderType _builder;
        int64_t _index = 0;
    };

    template<typename TType>
    class StringColumnBuilder : public IColumnBuilder {
    public:
        explicit StringColumnBuilder(std::shared_ptr<arrow::Array> array) : _array(make_istring_array(array)) {
        }

        arrow::Status Reserve(int64_t size) final {
            return _builder.Reserve(size);
        }

        arrow::Status Append(int64_t source_index) final {
            if (_array->array().IsNull(source_index)) {
                return _builder.AppendNull();
            }
            auto v = _array->Value(source_index);
            return _builder.Append(v);
        }

        std::shared_ptr<arrow::Array> Finish() final {
            std::shared_ptr<arrow::Array> array;
            auto status = _builder.Finish(&array);
            return array;
        }

    private:
        typename arrow::TypeTraits<TType>::BuilderType _builder;
        std::shared_ptr<IStringArray> _array;
    };

    template <typename TIndexType>
    class StringDictColumnBuilder : public IColumnBuilder {
    public:
        explicit StringDictColumnBuilder(std::shared_ptr<arrow::DictionaryArray> array) : _array(array), _index_builder(array->indices()) {
        }

        arrow::Status Reserve(int64_t size) final {
            return _index_builder.Reserve(size);
        }

        arrow::Status Append(int64_t source_index) final {
            return _index_builder.Append(source_index);
        }

        std::shared_ptr<arrow::Array> Finish() final {
            auto indices = _index_builder.Finish();
            auto dict = _array->dictionary();
            auto type = arrow::dictionary(indices->type(), dict->type());
            return std::make_shared<arrow::DictionaryArray>(type, indices, dict);
        }

    private:
        std::shared_ptr<arrow::DictionaryArray> _array;
        NumericColumnBuilder<TIndexType> _index_builder;
    };

    template <typename TType>
    arrow::Status batch_by_index(const std::shared_ptr<arrow::RecordBatch>& batch, std::shared_ptr<arrow::Array> index, std::shared_ptr<arrow::RecordBatch>* sorted_batch) {
        std::vector<std::shared_ptr<IColumnBuilder>> builders(batch->num_columns());
        for (int32_t i = 0; i < batch->num_columns(); i++) {
            auto array = batch->column(i);
            switch (array->type_id()) {
                case arrow::Type::INT8:
                    builders[i] = std::make_shared<NumericColumnBuilder<arrow::Int8Type>>(array);
                    break;
                case arrow::Type::INT16:
                    builders[i] = std::make_shared<NumericColumnBuilder<arrow::Int16Type>>(array);
                    break;
                case arrow::Type::INT32:
                    builders[i] = std::make_shared<NumericColumnBuilder<arrow::Int32Type>>(array);
                    break;
                case arrow::Type::INT64:
                    builders[i] = std::make_shared<NumericColumnBuilder<arrow::Int64Type>>(array);
                    break;
                case arrow::Type::UINT8:
                    builders[i] = std::make_shared<NumericColumnBuilder<arrow::UInt8Type>>(array);
                    break;
                case arrow::Type::UINT16:
                    builders[i] = std::make_shared<NumericColumnBuilder<arrow::UInt16Type>>(array);
                    break;
                case arrow::Type::UINT32:
                    builders[i] = std::make_shared<NumericColumnBuilder<arrow::UInt32Type>>(array);
                    break;
                case arrow::Type::UINT64:
                    builders[i] = std::make_shared<NumericColumnBuilder<arrow::UInt64Type>>(array);
                    break;
                case arrow::Type::HALF_FLOAT:
                    builders[i] = std::make_shared<NumericColumnBuilder<arrow::HalfFloatType>>(array);
                    break;
                case arrow::Type::FLOAT:
                    builders[i] = std::make_shared<NumericColumnBuilder<arrow::FloatType>>(array);
                    break;
                case arrow::Type::DOUBLE:
                    builders[i] = std::make_shared<NumericColumnBuilder<arrow::DoubleType>>(array);
                    break;
                case arrow::Type::STRING:
                    builders[i] = std::make_shared<StringColumnBuilder<arrow::StringType>>(array);
                    break;
                case arrow::Type::LARGE_STRING:
                    builders[i] = std::make_shared<StringColumnBuilder<arrow::LargeStringType>>(array);
                    break;
                case arrow::Type::DICTIONARY: {
                    auto dict_array = std::static_pointer_cast<arrow::DictionaryArray>(array);
                    auto dict = dict_array->dictionary();
                    auto indices = dict_array->indices();
                    switch (indices->type_id()) {
                        case arrow::Type::INT8:
                            builders[i] = std::make_shared<StringDictColumnBuilder<arrow::Int8Type>>(dict_array);
                            break;
                        case arrow::Type::INT16:
                            builders[i] = std::make_shared<StringDictColumnBuilder<arrow::Int16Type>>(dict_array);
                            break;
                        case arrow::Type::INT32:
                            builders[i] = std::make_shared<StringDictColumnBuilder<arrow::Int32Type>>(dict_array);
                            break;
                        default:
                            return arrow::Status::Invalid("Invalid dict index type " + indices->type()->ToString());
                    }
                    break;
                }
                default:
                    return arrow::Status::Invalid("Cannot sort array of type " + array->type()->ToString());
            }
        }

        auto index_array = std::static_pointer_cast<typename arrow::TypeTraits<TType>::ArrayType>(index);

        for (int64_t i = 0; i < index->length(); i++) {
            auto from_index = index_array->Value(i);
            for (auto& builder: builders) {
                ARROW_RETURN_NOT_OK(builder->Append(from_index));
            }
        }
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (size_t i = 0; i < builders.size(); i++) {
            arrays.push_back(builders[i]->Finish());
            fields.push_back(arrow::field(batch->column_name(i), (*arrays.rbegin())->type()));
        }

        *sorted_batch = arrow::RecordBatch::Make(arrow::schema(fields), index->length(), arrays);

        return arrow::Status::OK();
    }

    static arrow::Status batch_by_index(const std::shared_ptr<arrow::RecordBatch>& batch, std::shared_ptr<arrow::Array> index, std::shared_ptr<arrow::RecordBatch>* sorted_batch) {
        switch (index->type_id()) {
            case arrow::Type::INT8:
                return batch_by_index<arrow::Int8Type>(batch, index, sorted_batch);
            case arrow::Type::INT16:
                return batch_by_index<arrow::Int16Type>(batch, index, sorted_batch);
            case arrow::Type::INT32:
                return batch_by_index<arrow::Int32Type>(batch, index, sorted_batch);
            default:
                return arrow::Status::Invalid("Unexpected index type: " + index->type()->ToString());
        }
    }

    static inline arrow::Status sort(const std::shared_ptr<arrow::RecordBatch>& batch, std::vector<std::string> sort_columns, std::shared_ptr<arrow::RecordBatch>* sorted_batch) {
        std::shared_ptr<arrow::Array> index;
        ARROW_RETURN_NOT_OK(make_index(batch, sort_columns, &index));
        return batch_by_index(batch, index, sorted_batch);
    }
}

#endif //MARROW_SORT_H
