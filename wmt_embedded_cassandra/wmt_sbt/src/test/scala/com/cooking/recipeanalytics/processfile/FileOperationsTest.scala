package com.cooking.recipeanalytics.processfile

import java.io.File

import org.scalatest.FunSuite

class FileOperationsTest extends FunSuite {
  val fileOperations = new FileOperations

  test("testCopyFileToHdfs") {
val isFileCopied = fileOperations.copyFileToHdfs("/home/cloudera/IN/cooking/recipe/data//home/cloudera/IN/cooking/recipe/data","/user/cloudera/wmt/cooking/data/")
    assert(isFileCopied===true)
  }


  test("testArchiveToHdfs") {
    val isFileaArchived = fileOperations.archiveToHdfs("/home/cloudera/IN/cooking/recipe/data/","/user/cloudera/wmt/cooking/archive/")
    assert(isFileaArchived===true)
  }

  test("testGetListOfFiles") {
    assert(fileOperations.getListOfFiles(new File("/home/cloudera/IN/cooking/recipe/data/"),List("csv")).length===2)

  }

  test("testGetFileName") {
    assert(fileOperations.getFileName("/home/cloudera/IN/cooking/recipe/data/2018-01-09_11_recipe_data_raw.csv")==="2018-01-09_11_recipe_data_raw.csv")

  }

}
