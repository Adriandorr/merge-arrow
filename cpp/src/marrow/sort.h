//
// Created by adorr on 11/01/2020.
//

#ifndef MARROW_SORT_H
#define MARROW_SORT_H

#include <utility>
#include "marrow/index.h"

namespace marrow {

    template <typename TIndexType, typename TArrayType>
    arrow::Status array_by_index(std::shared_ptr<TIndexType> index, std::shared_ptr<TArrayType> array, std::shared_ptr<arrow::Array>* array_out) {
        typename arrow::TypeTraits<typename TArrayType::TypeClass>::BuilderType builder;
        for (int64_t i = 0; i < index->length(); i++) {
            auto ai = index->Value(i);
            if (ai < 0 || array->IsNull(ai)) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
            }
            else {
                ARROW_RETURN_NOT_OK(builder.Append(array->Value(ai)));
            }
        }
        return builder.Finish(array_out);
    }

    template <typename TBuilderType, typename TIndexType>
    arrow::Status array_by_index(std::shared_ptr<TIndexType> index, std::shared_ptr<IStringArray> array, std::shared_ptr<arrow::Array>* array_out) {
        TBuilderType builder;
        for (int64_t i = 0; i < index->length(); i++) {
            auto ai = index->Value(i);
            if (ai < 0 || array->array().IsNull(ai)) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
            }
            else {
                ARROW_RETURN_NOT_OK(builder.Append(array->Value(ai)));
            }
        }
        return builder.Finish(array_out);
    }

    template <typename TType>
    arrow::Status batch_by_index(const std::shared_ptr<arrow::RecordBatch>& batch, std::shared_ptr<arrow::Array> index, std::shared_ptr<arrow::RecordBatch>* sorted_batch) {
        std::vector<std::shared_ptr<arrow::Array>> sorted_arrays(batch->num_columns());
        auto typed_index = std::static_pointer_cast<typename arrow::TypeTraits<TType>::ArrayType>(index);
        for (int32_t i = 0; i < batch->num_columns(); i++) {
            auto array = batch->column(i);
            std::shared_ptr<arrow::Array> sorted_array;
            switch (array->type_id()) {
                case arrow::Type::INT8:
                    ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::Int8Array>(array), &sorted_array)));
                    break;
                case arrow::Type::INT16:
                    ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::Int16Array>(array), &sorted_array)));
                    break;
                case arrow::Type::INT32:
                    ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::Int32Array>(array), &sorted_array)));
                    break;
                case arrow::Type::INT64:
                    ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::Int64Array>(array), &sorted_array)));
                    break;
                case arrow::Type::UINT8:
                    ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::UInt8Array>(array), &sorted_array)));
                    break;
                case arrow::Type::UINT16:
                    ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::UInt16Array>(array), &sorted_array)));
                    break;
                case arrow::Type::UINT32:
                    ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::UInt32Array>(array), &sorted_array)));
                    break;
                case arrow::Type::UINT64:
                    ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::UInt64Array>(array), &sorted_array)));
                    break;
                case arrow::Type::HALF_FLOAT:
                    ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::HalfFloatArray>(array), &sorted_array)));
                    break;
                case arrow::Type::FLOAT:
                    ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::FloatArray>(array), &sorted_array)));
                    break;
                case arrow::Type::DOUBLE:
                    ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::DoubleArray>(array), &sorted_array)));
                    break;
                case arrow::Type::STRING:
                    ARROW_RETURN_NOT_OK((array_by_index<arrow::StringBuilder>(typed_index, make_istring_array(array), &sorted_array)));
                    break;
                case arrow::Type::LARGE_STRING:
                    ARROW_RETURN_NOT_OK((array_by_index<arrow::LargeStringBuilder>(typed_index ,make_istring_array(array), &sorted_array)));
                    break;
                case arrow::Type::DICTIONARY: {
                    auto dict_array = std::static_pointer_cast<arrow::DictionaryArray>(array);
                    auto indices = dict_array->indices();
                    switch (indices->type_id()) {
                        case arrow::Type::INT8:
                            ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::Int8Array>(indices), &indices)));
                            break;
                        case arrow::Type::INT16:
                            ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::Int16Array>(indices), &indices)));
                            break;
                        case arrow::Type::INT32:
                            ARROW_RETURN_NOT_OK((array_by_index(typed_index, std::static_pointer_cast<arrow::Int32Array>(indices), &indices)));
                            break;
                        default:
                            return arrow::Status::Invalid("Invalid dict index type " + indices->type()->ToString());
                    }
                    auto dict = dict_array->dictionary();
                    auto type = arrow::dictionary(indices->type(), dict->type());
                    sorted_array = std::make_shared<arrow::DictionaryArray>(type, indices, dict);
                    break;
                }
                default:
                    return arrow::Status::Invalid("Cannot sort array of type " + array->type()->ToString());
            }
            sorted_arrays[i] = sorted_array;
        }

        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (size_t i = 0; i < sorted_arrays.size(); i++) {
            fields.push_back(arrow::field(batch->column_name(i), sorted_arrays[i]->type()));
        }

        *sorted_batch = arrow::RecordBatch::Make(arrow::schema(fields), index->length(), sorted_arrays);

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
