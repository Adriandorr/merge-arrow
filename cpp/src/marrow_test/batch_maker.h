//
// Created by adorr on 11/01/2020.
//

#ifndef MARROW_BATCH_MAKER_H
#define MARROW_BATCH_MAKER_H

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/type_traits.h>
#include <arrow/record_batch.h>
#include <arrow/compute/api.h>

class BatchMaker {
public:
    template<class TBuilderType = arrow::Int32Builder, class TCType>
    BatchMaker& add_array_impl(std::string name, const std::vector<TCType> values, TCType null_value) {
        TBuilderType builder;
        for (auto v: values) {
            if (v == null_value) {
                throw_if_not_ok(builder.AppendNull());
            }
            else {
                throw_if_not_ok(builder.Append(v));
            }
        }
        std::shared_ptr<arrow::Array> array;
        throw_if_not_ok(builder.Finish(&array));
        _arrays.push_back(array);
        _fields.push_back(arrow::field(name, array->type()));
        return *this;
    }


    template<class TType = arrow::Int32Type>
    BatchMaker& add_array(std::string name, std::vector<typename TType::c_type> values, typename TType::c_type null_value = 0) {
        typedef typename arrow::TypeTraits<TType>::BuilderType BuilderType;
        return add_array_impl<BuilderType>(name, values, null_value);
    }

    template<class TType = arrow::Int32Type>
    BatchMaker& add_dict_array(std::string name, std::initializer_list<std::string> values, typename std::string null_value = "") {
        arrow::StringDictionaryBuilder builder;
        for (auto v: values) {
            if (v == null_value) {
                throw_if_not_ok(builder.AppendNull());
            }
            else {
                throw_if_not_ok(builder.Append(v));
            }
        }
        std::shared_ptr<arrow::Array> array;
        throw_if_not_ok(builder.Finish(&array));
        auto dict = std::static_pointer_cast<arrow::DictionaryArray>(array);
        auto indices = dict->indices();
        if (indices->type_id() != TType::type_id) {
            arrow::compute::FunctionContext ctx;
            throw_if_not_ok(arrow::compute::Cast(&ctx, *indices, std::make_shared<TType>(), arrow::compute::CastOptions(), &indices));
            auto type = arrow::dictionary(indices->type(), dict->dictionary()->type());
            throw_if_not_ok(arrow::DictionaryArray::FromArrays(type, indices, dict->dictionary(), &array));
        }
        _arrays.push_back(array);
        _fields.push_back(arrow::field(name, array->type()));
        return *this;
    }

    template<class TType = arrow::StringType>
    BatchMaker& add_string_array(std::string name, const std::vector<std::string>& values, typename std::string null_value = "") {
        typename arrow::TypeTraits<TType>::BuilderType builder;
        for (auto v: values) {
            if (v == null_value) {
                throw_if_not_ok(builder.AppendNull());
            }
            else {
                throw_if_not_ok(builder.Append(v));
            }
        }
        std::shared_ptr<arrow::Array> array;
        throw_if_not_ok(builder.Finish(&array));
        _arrays.push_back(array);
        _fields.push_back(arrow::field(name, array->type()));
        return *this;
    }

    std::shared_ptr<arrow::RecordBatch> record_batch() {
        auto schema = arrow::schema(_fields);
        auto batch = arrow::RecordBatch::Make(schema, _arrays[0]->length(), _arrays);
        _fields.clear();
        _arrays.clear();
        return batch;
    }

    template<typename TArray = arrow::Array>
    std::shared_ptr<TArray> array() {
        return std::dynamic_pointer_cast<TArray>(record_batch()->column(0));
    }

private:
    void throw_if_not_ok(const arrow::Status& status) const {
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
    }
    std::vector<std::shared_ptr<arrow::Field>> _fields;
    std::vector<std::shared_ptr<arrow::Array>> _arrays;
};
#endif //MARROW_BATCH_MAKER_H
