package com.anthem.ephc.coca.dfr.sjdbc.util

import java.util.Scanner
import org.apache.commons.codec.binary.Base64
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

object PasswordEncryption {

  def main(args: Array[String]) {
    val scanner: Scanner = new Scanner(System.in);
    System.out.println("Enter the string to be encrypted ");
    val temp: String = scanner.next();
    val encrypted_value = encrypt(temp)
    println("Encrypted value " + encrypted_value)
  }

  def encrypt(value: String): String =
    {
      val key: String = "1234567887654321"
      val initvector: String = "RandomInitVector"
      val iv: IvParameterSpec = new IvParameterSpec(initvector.getBytes("UTF-8"))
      val skeyspec: SecretKeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES")
      val cipher: Cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
      cipher.init(Cipher.ENCRYPT_MODE, skeyspec, iv)
      return Base64.encodeBase64String(cipher.doFinal(value.getBytes()))
    }

  def decrypt(encrypted_value: String): String =
    {
      val key: String = "1234567887654321"
      val initvector: String = "RandomInitVector"
      val iv: IvParameterSpec = new IvParameterSpec(initvector.getBytes("UTF-8"))
      val skeyspec: SecretKeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES")
      val cipher: Cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
      cipher.init(Cipher.DECRYPT_MODE, skeyspec, iv)
      val decrypted_value: String = new String(cipher.doFinal(Base64.decodeBase64(encrypted_value)))
      return (decrypted_value)

    }

}
