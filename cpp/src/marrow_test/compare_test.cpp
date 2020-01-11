//
// Created by adorr on 11/01/2020.
//

#include "gtest/gtest.h"
#include "batch_maker.h"
#include "marrow/compare.h"
#include <arrow/type_traits.h>


class CompareTest : public testing::Test {
public:
};

TEST(CompareTest, TestSimple) {
    auto array =  BatchMaker().add_array<arrow::Int32Type>("a", {1, 2, 1, 4, 5, 3}).array<arrow::Int32Array>();
    marrow::SimpleComparer<arrow::Int32Array> comparer(array);
    ASSERT_TRUE(comparer(0, 1));
    ASSERT_FALSE(comparer(0, 2));
    ASSERT_FALSE(comparer(1, 2));
    ASSERT_FALSE(comparer(1, 0));
    ASSERT_FALSE(comparer(0, 2));
    ASSERT_TRUE(comparer(2, 1));
}

template<typename TArrowType>
class CompareScalarTest : public testing::Test {

};

using ScalarTypes = ::testing::Types<arrow::Int8Type, arrow::Int16Type, arrow::Int32Type, arrow::Int64Type,
                                    arrow::UInt8Type, arrow::UInt16Type, arrow::UInt32Type, arrow::UInt64Type,
                                    arrow::HalfFloatType, arrow::FloatType, arrow::DoubleType>;

TYPED_TEST_CASE(CompareScalarTest, ScalarTypes);

TYPED_TEST(CompareScalarTest, TestWithoutNulls) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_array<TypeParam>("", {1, 2, 1, 4}).array();
    SCOPED_TRACE(array->ToString());
    marrow::SimpleComparer<typename TypeTrait::ArrayType> comparer(array);
    ASSERT_TRUE(comparer(0, 1));
    ASSERT_FALSE(comparer(0, 2));
    ASSERT_FALSE(comparer(1, 2));
    ASSERT_FALSE(comparer(1, 0));
    ASSERT_FALSE(comparer(0, 2));
    ASSERT_TRUE(comparer(2, 1));
}

TYPED_TEST(CompareScalarTest, TestWitNulls) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_array<TypeParam>("", {1, 2, 0, 4}).array();
    SCOPED_TRACE(array->ToString());
    marrow::SimpleComparer<typename TypeTrait::ArrayType> comparer(array);
    ASSERT_TRUE(comparer(0, 1));
    ASSERT_FALSE(comparer(0, 2));
    ASSERT_FALSE(comparer(1, 2));
    ASSERT_FALSE(comparer(1, 0));
    ASSERT_TRUE(comparer(2, 0));
    ASSERT_TRUE(comparer(2, 1));
}

TYPED_TEST(CompareScalarTest, TestMakeComparerwithNulls) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_array<TypeParam>("", {1, 2, 0, 4}).array();
    SCOPED_TRACE(array->ToString());
    auto comparer = marrow::make_array_comparer(array);
    {
        SCOPED_TRACE(typeid(*comparer).name());
        ASSERT_EQ(typeid(*comparer), typeid(marrow::NullComparer));
    }
    ASSERT_TRUE((*comparer)(0, 1));
    ASSERT_FALSE((*comparer)(0, 2));
    ASSERT_FALSE((*comparer)(1, 2));
    ASSERT_FALSE((*comparer)(1, 0));
    ASSERT_TRUE((*comparer)(2, 0));
    ASSERT_TRUE((*comparer)(2, 1));
}

TYPED_TEST(CompareScalarTest, TestMakeComparer) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_array<TypeParam>("", {1, 2, 1, 4}).array();
    SCOPED_TRACE(array->ToString());
    auto comparer = marrow::make_array_comparer(array);
    {
        SCOPED_TRACE(typeid(*comparer).name());
        ASSERT_EQ(typeid(*comparer), typeid(marrow::SimpleComparer<typename TypeTrait::ArrayType>));
    }
}

template<typename TArrowType>
class CompareStringDictTest : public testing::Test {

};

using DictIndexTypes = ::testing::Types<arrow::Int8Type, arrow::Int16Type, arrow::Int32Type>;


TYPED_TEST_CASE(CompareStringDictTest, DictIndexTypes);

TYPED_TEST(CompareStringDictTest, TestWithoutNull) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_dict_array<TypeParam>("", {"1", "2", "1", "4"}).array();
    SCOPED_TRACE(array->ToString());
    auto comparer = marrow::make_array_comparer(array);
    {
        SCOPED_TRACE(typeid(*comparer).name());
        ASSERT_EQ(typeid(*comparer), typeid(marrow::StringComparer));
    }
    ASSERT_TRUE((*comparer)(0, 1));
    ASSERT_FALSE((*comparer)(0, 2));
    ASSERT_FALSE((*comparer)(1, 2));
    ASSERT_FALSE((*comparer)(1, 0));
    ASSERT_FALSE((*comparer)(0, 2));
    ASSERT_TRUE((*comparer)(2, 1));
    ASSERT_FALSE((*comparer)(2, 0));
}

TYPED_TEST(CompareStringDictTest, TestWithNull) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_dict_array<TypeParam>("", {"1", "2", "", "4"}).array();
    SCOPED_TRACE(array->ToString());
    auto comparer = marrow::make_array_comparer(array);
    ASSERT_TRUE((*comparer)(0, 1));
    ASSERT_FALSE((*comparer)(0, 2));
    ASSERT_FALSE((*comparer)(1, 2));
    ASSERT_FALSE((*comparer)(1, 0));
    ASSERT_FALSE((*comparer)(0, 2));
    ASSERT_TRUE((*comparer)(2, 0));
}

template<typename TArrowType>
class CompareStringTest : public testing::Test {

};

using StringTypes = ::testing::Types<arrow::StringType, arrow::LargeStringType>;


TYPED_TEST_CASE(CompareStringTest, StringTypes);

TYPED_TEST(CompareStringTest, TestWithoutNull) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_string_array<TypeParam>("", {"1", "2", "1", "4"}).array();
    SCOPED_TRACE(array->ToString());
    auto comparer = marrow::make_array_comparer(array);
    ASSERT_EQ(typeid(*comparer), typeid(marrow::StringComparer));
    ASSERT_TRUE((*comparer)(0, 1));
    ASSERT_FALSE((*comparer)(0, 2));
    ASSERT_FALSE((*comparer)(1, 2));
    ASSERT_FALSE((*comparer)(1, 0));
    ASSERT_FALSE((*comparer)(0, 2));
    ASSERT_TRUE((*comparer)(2, 1));
    ASSERT_FALSE((*comparer)(2, 0));
}

TYPED_TEST(CompareStringTest, TestWithNull) {
    typedef arrow::TypeTraits<TypeParam> TypeTrait;
    auto array = BatchMaker().add_string_array<TypeParam>("", {"1", "2", "", "4"}).array();
    SCOPED_TRACE(array->ToString());
    auto comparer = marrow::make_array_comparer(array);
    ASSERT_TRUE((*comparer)(0, 1));
    ASSERT_FALSE((*comparer)(0, 2));
    ASSERT_FALSE((*comparer)(1, 2));
    ASSERT_FALSE((*comparer)(1, 0));
    ASSERT_FALSE((*comparer)(0, 2));
    ASSERT_TRUE((*comparer)(2, 0));
}

class TextColumnComparer : public testing::Test {

};

TEST_F(TextColumnComparer, TestMultiColumns) {
    auto batch = BatchMaker()
            .add_array<>("b", {100, 150, 99, 200, 100})
            .add_array<>("c", {5, 4, 3, 2, 1})
            .add_string_array<>("a", {"1", "2", "2", "2", "4"})
            .record_batch();
    auto comparer = marrow::make_comparer(batch, {"a", "b"});
    ASSERT_TRUE((*comparer)(0, 1));
    ASSERT_TRUE((*comparer)(0, 2));
    ASSERT_FALSE((*comparer)(1, 2));
    ASSERT_TRUE((*comparer)(2, 1));
    ASSERT_TRUE((*comparer)(1, 3));
    ASSERT_FALSE((*comparer)(3, 1));
    ASSERT_TRUE((*comparer)(2, 3));
    ASSERT_TRUE((*comparer)(2, 4));
    ASSERT_FALSE((*comparer)(4, 2));

}

TEST_F(TextColumnComparer, TestSingleColumns) {
    auto batch = BatchMaker()
            .add_array<>("b", {100, 150, 99, 200, 100})
            .add_array<>("c", {5, 4, 3, 2, 1})
            .add_string_array<>("a", {"1", "2", "2", "2", "4"})
            .record_batch();
    auto comparer = marrow::make_comparer(batch, {"a"});
    ASSERT_EQ(typeid(*comparer), typeid(marrow::StringComparer));
    ASSERT_TRUE((*comparer)(0, 1));
}
