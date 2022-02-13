package stu.cfl.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target(ElementType.FIELD)  //  作用于字段、枚举的常量
@Retention(RUNTIME)  // 注解会在class字节码文件中存在，在运行时可以通过反射获取到
public @interface TransientSink {

}
