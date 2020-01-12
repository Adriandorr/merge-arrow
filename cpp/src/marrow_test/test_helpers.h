//
// Created by adorr on 11/01/2020.
//

#ifndef MARROW_TEST_HELPERS_H
#define MARROW_TEST_HELPERS_H

#include <marrow/string_array.h>
#include <marrow/arrow_throw.h>

#define ASSERT_STATUS_OK(s) \
{                       \
    auto status = (s);  \
    SCOPED_TRACE(status.ToString());    \
    ASSERT_TRUE(s.ok());    \
}

static std::string compare_msg(std::shared_ptr<arrow::RecordBatch> actual, std::shared_ptr<arrow::RecordBatch> expected) {
    std::stringstream ret;
    for (int64_t i = 0; i < std::max(actual->num_columns(), expected->num_columns()); i++) {
        std::shared_ptr<arrow::Array> a, e;
        std::string a_name, e_name;
        if (i < actual->num_columns()) {
            a = actual->column(i);
            a_name = actual->column_name(i);
        }
        if (i < expected->num_columns()) {
            e = expected->column(i);
            e_name = expected->column_name(i);
        }
        ret << a_name;
        if (a && e) {
            ret << (a->Equals(*e) ? " == " : " != ");
        }
        ret << e_name << std::endl;

        if (a) {
            ret << "actual: " << std::endl << a->ToString() << std::endl;
        }
        if (e) {
            ret << "expected: " << std::endl << e->ToString() << std::endl;
        }
    }
    return ret.str();
}

static std::shared_ptr<arrow::RecordBatch> to_string_columns(std::shared_ptr<arrow::RecordBatch> batch) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto i = 0; i < batch->num_columns(); i++) {
        auto array = marrow::make_istring_array(batch->column(i));
        arrow::StringBuilder builder;
        for (int64_t i = 0; i < array->array().length(); i++) {
            if (array->array().IsNull(i)) {
                ARROW_THROW_NOT_OK(builder.AppendNull());
            }
            else {
                ARROW_THROW_NOT_OK(builder.Append(array->Value(i)))
            }
        }
        std::shared_ptr<arrow::Array> new_array;
        ARROW_THROW_NOT_OK(builder.Finish(&new_array));
        arrays.push_back(new_array);
        fields.push_back(arrow::field(batch->column_name(i), new_array->type()));
    }
    return arrow::RecordBatch::Make(arrow::schema(fields), batch->num_rows(), arrays);
}

using ScalarTypes = ::testing::Types<arrow::Int8Type, arrow::Int16Type, arrow::Int32Type, arrow::Int64Type,
        arrow::UInt8Type, arrow::UInt16Type, arrow::UInt32Type, arrow::UInt64Type,
        arrow::HalfFloatType, arrow::FloatType, arrow::DoubleType>;

using StringBuilderTypes = ::testing::Types<arrow::StringBuilder, arrow::LargeStringBuilder, arrow::StringDictionaryBuilder, arrow::StringDictionary32Builder>;

#endif //MARROW_TEST_HELPERS_H
