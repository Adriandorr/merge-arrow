//
// Created by adorr on 11/01/2020.
//

#ifndef MARROW_INDEX_H
#define MARROW_INDEX_H

#include <limits>
#include "marrow/compare.h"
#include <arrow/record_batch.h>
#include <arrow/builder.h>
#include <arrow/scalar.h>
#include <arrow/buffer.h>
#include <algorithm>

namespace marrow {

    template<typename TType = arrow::Int32Type>
    static arrow::Status make_index(std::shared_ptr<arrow::RecordBatch> batch, std::vector<std::string> index_columns, std::shared_ptr<arrow::Array>* index_out) {
        auto comparer = make_comparer(batch, index_columns);
        typedef arrow::TypeTraits<TType> TypeTrait;
        typedef typename TType::c_type c_type;

        std::shared_ptr<arrow::Buffer> buffer;
        ARROW_RETURN_NOT_OK(arrow::AllocateBuffer(arrow::default_memory_pool(), batch->num_rows() * sizeof(c_type), &buffer));

        auto it = reinterpret_cast<c_type*>(buffer->mutable_data());
        auto end = it + batch->num_rows();
        std::iota(it, end, 0);
        std::sort(it, end, [comparer](c_type i1, c_type i2) {return (*comparer)(i1, i2);});

        *index_out = std::make_shared<typename TypeTrait::ArrayType>(batch->num_rows(), buffer);
        return arrow::Status::OK();
    }

    static arrow::Status make_index(std::shared_ptr<arrow::RecordBatch> batch, std::vector<std::string> index_columns, std::shared_ptr<arrow::Array>* index_out) {
        if (batch->num_rows() <= std::numeric_limits<int8_t>::max()) {
            return make_index<arrow::Int8Type>(batch, index_columns, index_out);
        }
        else if (batch->num_rows() <= std::numeric_limits<int16_t>::max()) {
            return make_index<arrow::Int16Type>(batch, index_columns, index_out);
        }
        else if (batch->num_rows() <= std::numeric_limits<int8_t>::max()) {
            return make_index<arrow::Int32Type>(batch, index_columns, index_out);
        }

    }
}

#endif //MARROW_INDEX_H
