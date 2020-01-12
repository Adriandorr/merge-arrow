//
// Created by adorr on 12/01/2020.
//

#include "marrow/inner.h"
#include "marrow/arrow_throw.h"
#include "gtest/gtest.h"
#include "batch_maker.h"
#include "test_helpers.h"


template<typename TType>
class TestInnerMerge : public testing::Test {

};

TYPED_TEST_CASE(TestInnerMerge, ScalarTypes);

TYPED_TEST(TestInnerMerge, TestSimple) {
    auto batch1 = BatchMaker()
            .add_array<TypeParam>("a", {1, 1, 2, 3, 5}, 99)
            .template add_array<TypeParam>("b", {11, 12, 21, 31, 51})
            .record_batch();

    auto batch2 = BatchMaker()
            .add_array<TypeParam>("a", {1, 2, 4, 5, 5}, 99)
            .template add_array<TypeParam>("c", {11, 21, 41, 51, 52})
            .record_batch();

    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_STATUS_OK(marrow::inner(batch1, batch2, std::shared_ptr<arrow::Array>(), std::shared_ptr<arrow::Array>(), {"a"}, &actual));

    auto expected = BatchMaker()
            .add_array<TypeParam>("a", {1, 1, 2, 5, 5}, 99)
            .template add_array<TypeParam>("b", {11, 12, 21, 51, 51})
            .template add_array<TypeParam>("c", {11, 11, 21, 51, 52})
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

TYPED_TEST(TestInnerMerge, TestWithNulls) {
    auto batch1 = BatchMaker()
            .add_array<TypeParam>("a", {0, 0, 2, 3, 5})
            .template add_array<TypeParam>("b", {11, 12, 0, 31, 51})
            .record_batch();

    auto batch2 = BatchMaker()
            .add_array<TypeParam>("a", {0, 2, 4, 5, 5})
            .template add_array<TypeParam>("c", {11, 0, 41, 51, 52})
            .record_batch();

    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_STATUS_OK(marrow::inner(batch1, batch2, std::shared_ptr<arrow::Array>(), std::shared_ptr<arrow::Array>(), {"a"}, &actual));

    auto expected = BatchMaker()
            .add_array<TypeParam>("a", {0, 0, 2, 5, 5})
            .template add_array<TypeParam>("b", {11, 12, 0, 51, 51})
            .template add_array<TypeParam>("c", {11, 11, 0, 51, 52})
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

TYPED_TEST(TestInnerMerge, TestWithIndex) {
    auto batch1 = BatchMaker()
            .add_array<TypeParam>("a", {5, 3, 2, 1, 1}, 99)
            .template add_array<TypeParam>("b", {51, 32, 21, 11, 12})
            .record_batch();

    auto batch2 = BatchMaker()
            .add_array<TypeParam>("a", {4, 5, 5, 1, 2}, 99)
            .template add_array<TypeParam>("c", {41, 51, 52, 11, 21})
            .record_batch();

    std::shared_ptr<arrow::Array> index1, index2;
    ASSERT_STATUS_OK(marrow::make_index(batch1, {"a"}, &index1));
    ASSERT_STATUS_OK(marrow::make_index(batch2, {"a"}, &index2));
    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_STATUS_OK(marrow::inner(batch1, batch2, index1, index2, {"a"}, &actual));

    auto expected = BatchMaker()
            .add_array<TypeParam>("a", {1, 1, 2, 5, 5}, 99)
            .template add_array<TypeParam>("b", {11, 12, 21, 51, 51})
            .template add_array<TypeParam>("c", {11, 11, 21, 51, 52})
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

TYPED_TEST(TestInnerMerge, TestMultiColumns) {
    auto batch1 = BatchMaker()
            .add_array<TypeParam>("a", {1, 1, 2, 3, 3, 4, 5}, 99)
            .template add_array<TypeParam>("b", {11, 12, 21, 31, 31, 1, 15})
            .template add_array<TypeParam>("c", {10, 20, 30, 40, 50, 60, 70})
            .record_batch();

    auto batch2 = BatchMaker()
            .add_array<TypeParam>("a", {1, 1, 2, 3, 3, 5}, 99)
            .template add_array<TypeParam>("b", {10, 12, 22, 31, 31, 15})
            .template add_array<TypeParam>("c", {11, 21, 31, 41, 51, 61})
            .record_batch();

    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_STATUS_OK(marrow::inner(batch1, batch2, std::shared_ptr<arrow::Array>(), std::shared_ptr<arrow::Array>(), {"a", "b"}, &actual, "_right"));

    auto expected = BatchMaker()
            .add_array<TypeParam>("a", {1, 3, 3, 3, 3, 5}, 99)
            .template add_array<TypeParam>("b", {12, 31, 31, 31, 31, 15})
            .template add_array<TypeParam>("c", {20, 40, 40, 50, 50, 70})
            .template add_array<TypeParam>("c_right", {21, 41, 51, 41, 51, 61})
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

TYPED_TEST(TestInnerMerge, TestLeftHasMore) {
    auto batch1 = BatchMaker()
            .add_array<TypeParam>("a", {1, 1, 3, 5, 5}, 99)
            .template add_array<TypeParam>("b", {11, 12, 31, 51, 52})
            .record_batch();

    auto batch2 = BatchMaker()
            .add_array<TypeParam>("a", {2, 3, 4}, 99)
            .template add_array<TypeParam>("c", {21, 31, 41})
            .record_batch();

    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_STATUS_OK(marrow::inner(batch1, batch2, std::shared_ptr<arrow::Array>(), std::shared_ptr<arrow::Array>(), {"a"}, &actual));

    auto expected = BatchMaker()
            .add_array<TypeParam>("a", {3}, 99)
            .template add_array<TypeParam>("b", {31})
            .template add_array<TypeParam>("c", {31})
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

TYPED_TEST(TestInnerMerge, TestRightHasMore) {
    auto batch1 = BatchMaker()
            .add_array<TypeParam>("a", {2, 3, 4}, 99)
            .template add_array<TypeParam>("b", {21, 31, 41})
            .record_batch();

    auto batch2 = BatchMaker()
            .add_array<TypeParam>("a", {1, 1, 3, 5, 5}, 99)
            .template add_array<TypeParam>("c", {11, 12, 31, 51, 52})
            .record_batch();

    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_STATUS_OK(marrow::inner(batch1, batch2, std::shared_ptr<arrow::Array>(), std::shared_ptr<arrow::Array>(), {"a"}, &actual));

    auto expected = BatchMaker()
            .add_array<TypeParam>("a", {3}, 99)
            .template add_array<TypeParam>("b", {31})
            .template add_array<TypeParam>("c", {31})
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

TYPED_TEST(TestInnerMerge, TestEmptyResult) {
    auto batch1 = BatchMaker()
            .add_array<TypeParam>("a", {2, 3, 4}, 99)
            .template add_array<TypeParam>("b", {21, 31, 41})
            .record_batch();

    auto batch2 = BatchMaker()
            .add_array<TypeParam>("a", {1, 5}, 99)
            .template add_array<TypeParam>("c", {11, 51})
            .record_batch();

    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_STATUS_OK(marrow::inner(batch1, batch2, std::shared_ptr<arrow::Array>(), std::shared_ptr<arrow::Array>(), {"a"}, &actual));

    auto expected = BatchMaker()
            .add_array<TypeParam>("a", {}, 99)
            .template add_array<TypeParam>("b", {})
            .template add_array<TypeParam>("c", {})
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

template<typename TType>
class TestInnerMergeWithString : public testing::Test {

};

TYPED_TEST_CASE(TestInnerMergeWithString, StringBuilderTypes);

TYPED_TEST(TestInnerMergeWithString, TestSimple) {
    auto batch1 = BatchMaker()
            .add_array_impl<TypeParam, std::string>("a", {"1", "1", "2", "4", "5"}, "")
            .template add_array_impl<TypeParam, std::string>("b", {"1", "2", "3", "4", "5"}, "")
            .record_batch();
    auto batch2 = BatchMaker()
            .add_array_impl<TypeParam, std::string>("a", {"1", "2", "3", "5", "5"}, "")
            .template add_array_impl<TypeParam, std::string>("c", {"1", "2", "3", "4", "5"}, "")
            .record_batch();

    std::shared_ptr<arrow::RecordBatch> actual;
    ASSERT_STATUS_OK(marrow::inner(batch1, batch2, std::shared_ptr<arrow::Array>(), std::shared_ptr<arrow::Array>(), {"a"}, &actual));
    {
        auto fields = batch1->schema()->fields();
        fields.push_back(batch2->schema()->field(1));
        auto expected_schema = arrow::schema(fields);
        SCOPED_TRACE(actual->schema()->ToString());
        SCOPED_TRACE(expected_schema->ToString());
        ASSERT_TRUE(actual->schema()->Equals(*expected_schema));
    }
    //Convert to string column as comparing dictionary columns with differently sorted dictionaries  retunres false.
    actual = to_string_columns(actual);

    auto expected = BatchMaker()
            .add_array_impl<arrow::StringBuilder, std::string>("a", {"1", "1", "2", "5", "5"}, "")
            .template add_array_impl<arrow::StringBuilder, std::string>("b", {"1", "2", "3", "5", "5"}, "")
            .template add_array_impl<arrow::StringBuilder, std::string>("c", {"1", "1", "2", "4", "5"}, "")
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));

}