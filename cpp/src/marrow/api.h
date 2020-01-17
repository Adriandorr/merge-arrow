//
// Created by adorr on 16/01/2020.
//

#ifndef MARROW_API_H
#define MARROW_API_H

#include "index.h"
#include "sort.h"
#include "left.h"
#include "inner.h"
#include "outer.h"
#include "arrow_throw.h"
#include <arrow/util/key_value_metadata.h>
#include <boost/algorithm/string.hpp>

namespace marrow {
    constexpr const char* index_column_name = "__marrow_index";
    constexpr const char* index_metadata_key = "__marrow_index";
    constexpr const char* sort_metadata_key = "__marrow_index";

    std::shared_ptr<arrow::RecordBatch> add_sort_metadata(std::shared_ptr<arrow::RecordBatch> batch, std::vector<std::string> on) {
        std::string ret = on[0];
        for (size_t i = 1; i < on.size(); i++) {
            ret += ",";
            ret += on[i];
        }
        std::unordered_map<std::string, std::string> meta_data;
        if (batch->schema()->metadata()) {
            batch->schema()->metadata()->ToUnorderedMap(&meta_data);
        }
        meta_data[index_metadata_key] = ret;

        return batch->ReplaceSchemaMetadata(arrow::key_value_metadata(meta_data));
    }

    std::pair<std::shared_ptr<arrow::Array>, std::shared_ptr<arrow::RecordBatch>> get_index(std::shared_ptr<arrow::RecordBatch> batch, std::vector<std::string> on) {
        std::shared_ptr<arrow::Array> index;
        if (batch->schema()->metadata()) {
            auto value_index = batch->schema()->metadata()->FindKey(index_metadata_key);
            if (value_index >= 0) {
                auto value = batch->schema()->metadata()->value(value_index);
                std::vector<std::string> sort_columns;
                boost::split(sort_columns, value, [](char c) { return c == ','; });
                if (sort_columns.size() >= on.size()) {
                    sort_columns.resize(on.size());
                    if (std::equal(sort_columns.begin(), sort_columns.end(), on.begin(), on.end())) {
                        auto ret_index = batch->schema()->GetFieldIndex(index_column_name);
                        if (ret_index >= 0) {
                            //It has an index, remove and return
                            index = batch->column(ret_index);
                            ARROW_THROW_NOT_OK(batch->RemoveColumn(ret_index, &batch));
                        }
                        //no index but meta data, hence it is sorted
                        return {index, batch};
                    }
                }
            }
        }
        auto index_index = batch->schema()->GetFieldIndex(index_column_name);
        if (index_index >= 0) {
            //It has an index, remove and return
            ARROW_THROW_NOT_OK(batch->RemoveColumn(index_index, &batch));
        }
        // No usable index or sort order, so create an index
        ARROW_THROW_NOT_OK(make_index(batch, on, &index));
        return {index, batch};
    }

    namespace api {

    std::shared_ptr<arrow::RecordBatch> add_index(std::shared_ptr<arrow::RecordBatch> batch, std::vector<std::string> on) {
        std::shared_ptr<arrow::Array> index;
        ARROW_THROW_NOT_OK(make_index(batch, on, &index));
        auto field = arrow::field(index_column_name, index->type());
        ARROW_THROW_NOT_OK(batch->AddColumn(0, field, index, &batch));
        return add_sort_metadata(batch, on);
    }

    std::shared_ptr<arrow::RecordBatch> sort(std::shared_ptr<arrow::RecordBatch> batch, std::vector<std::string> on) {
        auto index = get_index(batch, on);
        ARROW_THROW_NOT_OK(batch_by_index(index.second, index.first, &batch));
        return batch;
    }

    std::shared_ptr<arrow::RecordBatch> merge(std::shared_ptr<arrow::RecordBatch> batch1, std::shared_ptr<arrow::RecordBatch> batch2, std::vector<std::string> on, std::string how, std::string right_prefix) {
        auto index1 = get_index(batch1, on);
        auto index2 = get_index(batch2, on);
        std::shared_ptr<arrow::RecordBatch> ret;
        if (how == "left") {
            ARROW_THROW_NOT_OK(left(index1.second, index2.second, index1.first, index2.first, on, &ret, right_prefix));
        }
        else if (how == "inner") {
            ARROW_THROW_NOT_OK(inner(index1.second, index2.second, index1.first, index2.first, on, &ret, right_prefix));
        }
        else if (how == "outer") {
            ARROW_THROW_NOT_OK(outer(index1.second, index2.second, index1.first, index2.first, on, &ret, right_prefix));
        }
        else {
            throw std::runtime_error("Unsupported merge how argument: " + how);
        }

        return ret;
    }
    }
}

#endif //MARROW_API_H
