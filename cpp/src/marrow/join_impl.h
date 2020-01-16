//
// Created by adorr on 14/01/2020.
//

#ifndef MARROW_MERGE_H
#define MARROW_MERGE_H

#include <arrow/record_batch.h>
#include <arrow/builder.h>
#include <iostream>
#include "compare.h"
#include "sort.h"

namespace marrow {
    class IIndexRecordBatch {
    public:
        virtual ~IIndexRecordBatch() = default;
        virtual int64_t get_index(int64_t index) const = 0;
    };

    class SortedIndexRecordBatch : public IIndexRecordBatch {
    public:
        int64_t get_index(int64_t index) const {
            arrow::StringBuilder b;
            return index;
        }
    };

    template<typename TIndexArray>
    class IndexRecordBatch : public IIndexRecordBatch {
    public:
        IndexRecordBatch(std::shared_ptr<arrow::Array> index) : _index(std::dynamic_pointer_cast<TIndexArray>(index)) {
        }
        int64_t get_index(int64_t index) const {
            return _index->Value(index);
        }

    private:
        std::shared_ptr<TIndexArray> _index;
    };

static std::shared_ptr<IIndexRecordBatch> make_index(std::shared_ptr<arrow::Array> index) {
    if (index) {
        switch (index->type_id()) {
            case arrow::Type::INT8:
                return std::make_shared<IndexRecordBatch<arrow::Int8Array>>(index);
            case arrow::Type::INT16:
                return std::make_shared<IndexRecordBatch<arrow::Int16Array>>(index);
            case arrow::Type::INT32:
                return std::make_shared<IndexRecordBatch<arrow::Int32Array>>(index);
            case arrow::Type::INT64:
                return std::make_shared<IndexRecordBatch<arrow::Int64Array>>(index);
            default:
                throw std::runtime_error("Invalid index type: " + index->type()->ToString());
        }
    }
    static auto ret = std::make_shared<SortedIndexRecordBatch>();
    return ret;
}

template<typename TBuilderType, typename TArrayType>
arrow::Status unify_outer_column(std::shared_ptr<TArrayType> left_column, std::shared_ptr<TArrayType> right_column, std::shared_ptr<arrow::Array>* left_column_out) {
    TBuilderType builder;
    for (int64_t i = 0; i < left_column->length(); i++) {
        if (left_column->IsNull(i)) {
            if (right_column->IsNull(i)) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
            }
            else {
                ARROW_RETURN_NOT_OK(builder.Append(right_column->Value(i)));
            }
        }
        else {
            ARROW_RETURN_NOT_OK(builder.Append(left_column->Value(i)));
        }
    }
    return builder.Finish(left_column_out);
}


static inline arrow::Status unify_outer_on_columns(std::shared_ptr<arrow::RecordBatch> left, std::shared_ptr<arrow::RecordBatch> right, std::vector<std::string> on, std::shared_ptr<arrow::RecordBatch>* left_out, std::shared_ptr<arrow::RecordBatch>* right_out) {
        for (auto column_name: on) {
            auto left_column_index = left->schema()->GetFieldIndex(column_name);
            auto right_column_index = right->schema()->GetFieldIndex(column_name);
            auto left_column = left->column(left_column_index);
            auto right_column = right->column(right_column_index);
            ARROW_RETURN_IF(!left_column->type()->Equals(right_column->type()), arrow::Status::Invalid("Incompatible column type for column " + column_name));
            switch (left_column->type_id()) {
                case arrow::Type::INT8:
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::Int8Builder>(std::static_pointer_cast<arrow::Int8Array>(left_column), std::static_pointer_cast<arrow::Int8Array>(right_column), &left_column));
                    break;
                case arrow::Type::INT16:
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::Int16Builder>(std::static_pointer_cast<arrow::Int16Array>(left_column), std::static_pointer_cast<arrow::Int16Array>(right_column), &left_column));
                    break;
                case arrow::Type::INT32:
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::Int32Builder>(std::static_pointer_cast<arrow::Int32Array>(left_column), std::static_pointer_cast<arrow::Int32Array>(right_column), &left_column));
                    break;
                case arrow::Type::INT64:
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::Int64Builder>(std::static_pointer_cast<arrow::Int64Array>(left_column), std::static_pointer_cast<arrow::Int64Array>(right_column), &left_column));
                    break;
                case arrow::Type::UINT8:
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::UInt8Builder>(std::static_pointer_cast<arrow::UInt8Array>(left_column), std::static_pointer_cast<arrow::UInt8Array>(right_column), &left_column));
                    break;
                case arrow::Type::UINT16:
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::UInt16Builder>(std::static_pointer_cast<arrow::UInt16Array>(left_column), std::static_pointer_cast<arrow::UInt16Array>(right_column), &left_column));
                    break;
                case arrow::Type::UINT32:
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::UInt32Builder>(std::static_pointer_cast<arrow::UInt32Array>(left_column), std::static_pointer_cast<arrow::UInt32Array>(right_column), &left_column));
                    break;
                case arrow::Type::UINT64:
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::UInt64Builder>(std::static_pointer_cast<arrow::UInt64Array>(left_column), std::static_pointer_cast<arrow::UInt64Array>(right_column), &left_column));
                    break;
                case arrow::Type::HALF_FLOAT:
                    ARROW_RETURN_NOT_OK((unify_outer_column<arrow::HalfFloatBuilder>(std::static_pointer_cast<arrow::HalfFloatArray>(left_column), std::static_pointer_cast<arrow::HalfFloatArray>(right_column), &left_column)));
                    break;
                case arrow::Type::FLOAT:
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::FloatBuilder>(std::static_pointer_cast<arrow::FloatArray>(left_column), std::static_pointer_cast<arrow::FloatArray>(right_column), &left_column));
                    break;
                case arrow::Type::DOUBLE:
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::DoubleBuilder>(std::static_pointer_cast<arrow::DoubleArray>(left_column), std::static_pointer_cast<arrow::DoubleArray>(right_column), &left_column));
                    break;
                case arrow::Type::STRING:
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::StringBuilder>(make_istring_array(left_column), make_istring_array(right_column), &left_column));
                    break;
                case arrow::Type::LARGE_STRING:
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::LargeStringBuilder>(make_istring_array(left_column), make_istring_array(right_column), &left_column));
                    break;
                case arrow::Type::DICTIONARY: {
                    ARROW_RETURN_NOT_OK(unify_outer_column<arrow::StringDictionaryBuilder>(make_istring_array(left_column), make_istring_array(right_column), &left_column));
                    break;
                }
                default:
                    return arrow::Status::Invalid("Cannot sort array of type " + left_column->type()->ToString());
            }
            auto left_field = arrow::field(left->column_name(left_column_index), left_column->type());
            ARROW_RETURN_NOT_OK(left->RemoveColumn(left_column_index, &left));
            ARROW_RETURN_NOT_OK(left->AddColumn(left_column_index, left_field, left_column, &left));
            ARROW_RETURN_NOT_OK(right->RemoveColumn(right_column_index, &right));
            std::cout << "left: " << left->schema()->ToString() << std::endl;
            std::cout << "right: " << right->schema()->ToString() << std::endl;
        }
        *left_out = left;
        *right_out = right;
        return arrow::Status::OK();
    }

    template <typename TIndexBuilder>
    arrow::Status join_impl(std::shared_ptr<arrow::RecordBatch> left, std::shared_ptr<arrow::RecordBatch> right,
                            std::shared_ptr<arrow::Array> left_index_array,
                            std::shared_ptr<arrow::Array> right_index_array, std::vector<std::string> on,
                            std::shared_ptr<arrow::RecordBatch> *table_out, std::string right_prefix, bool is_outer = false) {
        auto left_index = make_index(left_index_array);
        auto right_index = make_index(right_index_array);
        TIndexBuilder index_builder;
        auto comparer = make_comparer(left, right, on);
        int64_t lindex = 0, rindex = 0, lend = left->num_rows(), rend = right->num_rows();
        while (lindex < lend && rindex < rend) {
            auto li = left_index->get_index(lindex);
            auto ri = right_index->get_index(rindex);
            if (comparer->lt(li, ri)) {
                ARROW_RETURN_NOT_OK(index_builder.left_only(li));
                lindex++;
            }
            else if (comparer->gt(li, ri)) {
                ARROW_RETURN_NOT_OK(index_builder.right_only(ri));
                rindex++;
            }
            else {
                int64_t li_end = lindex + 1;
                int64_t ri_end = rindex + 1;
                for (; li_end < lend; li_end++) {
                    auto li_temp = left_index->get_index(li_end);
                    if (comparer->gt(li_temp, ri)) {
                        break;
                    }
                }
                for (; ri_end < rend; ri_end++) {
                    auto ri_temp = right_index->get_index(ri_end);
                    if (comparer->lt(li, ri_temp)) {
                        break;
                    }
                }
                for (auto l = lindex; l < li_end; l++) {
                    for (auto r = rindex; r < ri_end; r++) {
                        ARROW_RETURN_NOT_OK(index_builder.both(left_index->get_index(l), right_index->get_index(r)));
                    }
                }
                lindex = li_end;
                rindex = ri_end;
            }
        }

        while (lindex < lend) {
            auto li = left_index->get_index(lindex);
            ARROW_RETURN_NOT_OK(index_builder.left_only(li));
            lindex++;
        }
        while (rindex < rend) {
            auto ri = right_index->get_index(rindex);
            ARROW_RETURN_NOT_OK(index_builder.right_only(ri));
            rindex++;
        }

        std::shared_ptr<arrow::Array> left_array,  right_array;
        ARROW_RETURN_NOT_OK(index_builder.finish(&left_array, &right_array));

        ARROW_RETURN_NOT_OK(batch_by_index(left, left_array, &left));

        for (int64_t i = 0; i < right->num_columns(); i++) {
            auto name = right->column_name(i);
            if (std::find(on.begin(), on.end(), name) != on.end()) {
                if (!is_outer) {
                    ARROW_RETURN_NOT_OK(right->RemoveColumn(i--, &right));
                }
            }
            else if (!right_prefix.empty()) {
                auto c = right->column(i);
                auto cn = right->column_name(i) + right_prefix;
                ARROW_RETURN_NOT_OK(right->RemoveColumn(i, &right));
                ARROW_RETURN_NOT_OK(right->AddColumn(i, arrow::field(cn, c->type()), c, &right));
            }
        }

        ARROW_RETURN_NOT_OK(batch_by_index(right, right_array, &right));

        if (is_outer) {
            //Unify the index columns from left/right. If left is a null, it should get the value from the right;
            ARROW_RETURN_NOT_OK(unify_outer_on_columns(left, right, on, &left, &right));

        }

        for (int64_t i = 0; i < right->num_columns(); i++) {
            ARROW_RETURN_NOT_OK(left->AddColumn(left->num_columns(), right->schema()->field(i), right->column(i), &left));
        }

        *table_out = left;
        return arrow::Status::OK();
    }

}
#endif //MARROW_MERGE_H
