//
// Created by adorr on 12/01/2020.
//

#ifndef MARROW_INNER_H
#define MARROW_INNER_H

#include <arrow/record_batch.h>
#include <arrow/array/builder_dict.h>
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

    std::shared_ptr<IIndexRecordBatch> make_index(std::shared_ptr<arrow::Array> index) {
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

    static arrow::Status inner(std::shared_ptr<arrow::RecordBatch> left, std::shared_ptr<arrow::RecordBatch> right, std::shared_ptr<arrow::Array> left_index_array,  std::shared_ptr<arrow::Array> right_index_array, std::vector<std::string> on, std::shared_ptr<arrow::RecordBatch>* table_out, std::string right_prefix = "") {
        auto left_index = make_index(left_index_array);
        auto right_index = make_index(right_index_array);

        arrow::AdaptiveIntBuilder left_builder, right_builder;
        auto comparer = make_comparer(left, right, on);
        int64_t lindex = 0, rindex = 0, lend = left->num_rows(), rend = right->num_rows();
        while (lindex < lend && rindex < rend) {
            auto li = left_index->get_index(lindex);
            auto ri = right_index->get_index(rindex);
            if (comparer->lt(li, ri)) {
                lindex++;
            }
            else if (comparer->gt(li, ri)) {
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
                        ARROW_RETURN_NOT_OK(left_builder.Append(left_index->get_index(l)));
                        ARROW_RETURN_NOT_OK(right_builder.Append(right_index->get_index(r)));
                    }
                }
                lindex = li_end;
                rindex = ri_end;
            }
        }
        std::shared_ptr<arrow::Array> left_array, right_array;
        ARROW_RETURN_NOT_OK(left_builder.Finish(&left_array));
        ARROW_RETURN_NOT_OK(right_builder.Finish(&right_array));

        ARROW_RETURN_NOT_OK(batch_by_index(left, left_array, &left));

        for (int64_t i = 0; i < right->num_columns(); i++) {
            auto name = right->column_name(i);
            if (std::find(on.begin(), on.end(), name) != on.end()) {
                ARROW_RETURN_NOT_OK(right->RemoveColumn(i--, &right));
            }
            else if (!right_prefix.empty()) {
                auto c = right->column(i);
                auto cn = right->column_name(i) + right_prefix;
                ARROW_RETURN_NOT_OK(right->RemoveColumn(i, &right));
                ARROW_RETURN_NOT_OK(right->AddColumn(i, arrow::field(cn, c->type()), c, &right));
            }
        }
        ARROW_RETURN_NOT_OK(batch_by_index(right, right_array, &right));
        for (int64_t i = 0; i < right->num_columns(); i++) {
            ARROW_RETURN_NOT_OK(left->AddColumn(left->num_columns(), right->schema()->field(i), right->column(i), &left));
        }

        *table_out = left;
        return arrow::Status::OK();
    }
}

#endif //MARROW_INNER_H
