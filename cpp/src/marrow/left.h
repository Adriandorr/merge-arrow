//
// Created by adorr on 14/01/2020.
//

#ifndef MARROW_LEFT_H
#define MARROW_LEFT_H

#include "join_impl.h"

namespace marrow {
    class LeftJoinBuilder {
    public:
        arrow::Status left_only(int64_t index) {
            ARROW_RETURN_NOT_OK(_lbuilder.Append(index));
            ARROW_RETURN_NOT_OK(_rbuilder.Append(-1));
            return arrow::Status::OK();
        }

        arrow::Status right_only(int64_t index) { return arrow::Status::OK(); }

        arrow::Status both(int64_t lindex, int64_t rindex) {
            ARROW_RETURN_NOT_OK(_lbuilder.Append(lindex));
            ARROW_RETURN_NOT_OK(_rbuilder.Append(rindex));
            return arrow::Status::OK();
        }

        arrow::Status finish(std::shared_ptr <arrow::Array> *left, std::shared_ptr <arrow::Array> *right) {
            ARROW_RETURN_NOT_OK(_lbuilder.Finish(left));
            return _rbuilder.Finish(right);
        }
    private:
        arrow::AdaptiveIntBuilder _lbuilder;
        arrow::AdaptiveIntBuilder _rbuilder;
    };

    static arrow::Status left(std::shared_ptr<arrow::RecordBatch> left, std::shared_ptr<arrow::RecordBatch> right, std::shared_ptr<arrow::Array> left_index_array,  std::shared_ptr<arrow::Array> right_index_array, std::vector<std::string> on, std::shared_ptr<arrow::RecordBatch>* table_out, std::string right_prefix = "") {
        return join_impl<LeftJoinBuilder>(left, right, left_index_array,  right_index_array, on, table_out, right_prefix); //h

    }
}
#endif //MARROW_LEFT_H
