//
// Created by adorr on 11/01/2020.
//

#ifndef MARROW_TEST_HELPERS_H
#define MARROW_TEST_HELPERS_H

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

using ScalarTypes = ::testing::Types<arrow::Int8Type, arrow::Int16Type, arrow::Int32Type, arrow::Int64Type,
        arrow::UInt8Type, arrow::UInt16Type, arrow::UInt32Type, arrow::UInt64Type,
        arrow::HalfFloatType, arrow::FloatType, arrow::DoubleType>;


#endif //MARROW_TEST_HELPERS_H
