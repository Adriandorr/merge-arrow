//
// Created by adorr on 11/01/2020.
//

#include "marrow/sort.h"
#include "marrow/arrow_throw.h"
#include "gtest/gtest.h"
#include "batch_maker.h"
#include "test_helpers.h"


template<typename TType>
class TestScalarSort : public testing::Test {

};

TYPED_TEST_CASE(TestScalarSort, ScalarTypes);

TYPED_TEST(TestScalarSort, TestSimple) {
    auto batch = BatchMaker()
            .add_array<TypeParam>("a", {2, 3, 1, 4, 0}, 99)
            .template add_array<TypeParam>("b", {1, 2, 3, 4, 5})
            .record_batch();

    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_STATUS_OK(marrow::sort(batch, {"a"}, &actual));

    auto expected = BatchMaker()
            .add_array<TypeParam>("a", {0, 1, 2, 3, 4}, 99)
            .template add_array<TypeParam>("b", {5, 3, 1, 2, 4})
            .record_batch();

    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

TYPED_TEST(TestScalarSort, TestWithNulls) {
    auto batch = BatchMaker()
            .add_array<TypeParam>("a", {2, 3, 1, 4, 0})
            .template add_array<TypeParam>("b", {1, 2, 3, 4, 5})
            .record_batch();

    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_STATUS_OK(marrow::sort(batch, {"a"}, &actual));

    auto expected = BatchMaker()
            .add_array<TypeParam>("a", {0, 1, 2, 3, 4})
            .template add_array<TypeParam>("b", {5, 3, 1, 2, 4})
            .record_batch();

    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

std::shared_ptr<arrow::RecordBatch> to_string_columns(std::shared_ptr<arrow::RecordBatch> batch) {
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

template<typename TType>
class TestStringSort : public testing::Test {
public:
};
using StringBuilderTypes = ::testing::Types<arrow::StringBuilder, arrow::LargeStringBuilder, arrow::StringDictionaryBuilder, arrow::StringDictionary32Builder>;

TYPED_TEST_CASE(TestStringSort, StringBuilderTypes);

TYPED_TEST(TestStringSort, TestSimple) {
    auto batch = BatchMaker()
            .add_array_impl<TypeParam, std::string>("a", {"2", "3", "1", "4", "0"}, "")
            .template add_array_impl<TypeParam, std::string>("b", {"1", "2", "3", "4", "5"}, "")
            .record_batch();

    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_STATUS_OK(marrow::sort(batch, {"a"}, &actual));
    {
        SCOPED_TRACE(actual->schema()->ToString());
        SCOPED_TRACE(batch->schema()->ToString());
        ASSERT_TRUE(actual->schema()->Equals(*batch->schema()));
    }
    //Convert to string column as comparing dictionary columns with differently sorted dictionaries is retunres false.
    actual = to_string_columns(actual);

    auto expected = BatchMaker()
            .add_array_impl<arrow::StringBuilder, std::string>("a", {"0", "1", "2", "3", "4"}, "")
            .template add_array_impl<arrow::StringBuilder, std::string>("b", {"5", "3", "1", "2", "4"}, "")
            .record_batch();

    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->ApproxEquals(*expected));
}

TYPED_TEST(TestStringSort, TestWithNulls) {
    auto batch = BatchMaker()
            .add_array_impl<TypeParam, std::string>("a", {"2", "3", "1", "4", ""}, "")
            .template add_array_impl<TypeParam, std::string>("b", {"1", "2", "3", "4", "5"}, "")
            .record_batch();

    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_STATUS_OK(marrow::sort(batch, {"a"}, &actual));

    actual = to_string_columns(actual);

    auto expected = BatchMaker()
            .add_array_impl<arrow::StringBuilder, std::string>("a", {"", "1", "2", "3", "4"}, "")
            .template add_array_impl<arrow::StringBuilder, std::string>("b", {"5", "3", "1", "2", "4"}, "")
            .record_batch();




    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->ApproxEquals(*expected));
}
